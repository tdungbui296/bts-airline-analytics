# BTS Airline Analytics Architecture

This document presents the technical architecture of the BTS Airline Analytics Engineering portfolio.

The project transforms 24 monthly US Bureau of Transportation Statistics Reporting Carrier On-Time Performance files covering 2024 and 2025 into a tested BigQuery dimensional warehouse and business-facing analytical marts.

Validated historical volume:

| Year | Monthly files | Flight records |
|---:|---:|---:|
| 2024 | 12 | 7,079,061 |
| 2025 | 12 | 7,001,619 |
| Total | 24 | 14,080,680 |

## 1. End-to-End Data Platform

```mermaid
flowchart TB
    source["BTS monthly ZIP files<br/>2024–2025 · 24 files"]

    subgraph local["Local Python preparation"]
        validate["Validate annual batches<br/>validate_batch.py"]
        project["Project 51-column core<br/>project_batch.py"]
        audit["Audit BigQuery types<br/>audit_types.py"]
        loadfiles["Generate deterministic CSV.GZ<br/>build_bq_load_files.py"]
    end

    subgraph evidence["Tracked validation evidence"]
        manifests["Source, processed, and load manifests<br/>row counts · sizes · SHA-256"]
        profiles["Profiling and anomaly evidence<br/>review · type · quality rules"]
    end

    subgraph bigquery["Google BigQuery · asia-southeast1"]
        raw["raw_bts.flights<br/>14,080,680 rows · 51 columns"]
        dbt["dbt transformation project<br/>11 models · 214 tests"]
        warehouse["Dimensional warehouse<br/>dimensions · facts · marts"]
    end

    bi["Power BI analytical layer"]

    source --> validate
    validate --> project
    project --> audit
    audit --> loadfiles
    loadfiles --> raw
    raw --> dbt
    dbt --> warehouse
    warehouse --> bi

    validate -.-> manifests
    project -.-> manifests
    loadfiles -.-> manifests
    validate -.-> profiles
    audit -.-> profiles
```

### Architecture responsibilities

| Layer | Responsibility |
|---|---|
| BTS source | Provides monthly reporting-carrier flight-performance records |
| Python validation | Verifies file presence, schema behavior, row counts, and known anomalies |
| Core projection | Standardizes each month into the ordered 51-column contract |
| Type audit | Confirms compatibility with the explicit BigQuery schema |
| Load-file generation | Creates deterministic compressed files and load manifests |
| BigQuery landing | Stores the validated 2024–2025 flight history |
| dbt | Applies semantic rules, dimensional modeling, tests, and reconciliation |
| Analytical marts | Expose stable business grains and reusable measures |
| Power BI | Presents carrier, route, airport, and delay-cause analysis |

The local raw, processed, and load datasets are excluded from Git. Compact manifests, profiling outputs, configuration contracts, tests, and execution logs remain tracked as reproducibility evidence.

## 2. Local Data-Preparation Pipeline

```mermaid
flowchart LR
    zip["Monthly source ZIP"]

    validation["Validation gate<br/>file · columns · rows · anomalies"]
    core["Processed core CSV.GZ<br/>51 standardized columns"]
    typecheck["Type-contract gate<br/>51-column audit"]
    bqfile["BigQuery-ready CSV.GZ<br/>deterministic gzip"]

    sourceManifest["Source manifest"]
    processedManifest["Processed manifest"]
    loadManifest["Load manifest"]

    zip --> validation
    validation --> core
    core --> typecheck
    typecheck --> bqfile

    validation -.-> sourceManifest
    core -.-> processedManifest
    bqfile -.-> loadManifest
```

### Validation gates

A reporting year is approved for BigQuery ingestion only when:

- all 12 expected monthly source files exist;
- source identities and hashes are understood;
- annual and monthly row counts reconcile;
- the projected files contain the ordered 51-column contract;
- the type audit covers all 51 columns;
- generated outputs match their manifest sizes and hashes;
- no interrupted `.tmp` files remain; and
- any review-status source conditions have supporting evidence.

The 2024 batch contains two reviewed months:

- July 2024
- August 2024

These months were approved through explicit anomaly investigation rather than silently treated as automatic passes.

## 3. BigQuery and dbt Transformation Layers

```mermaid
flowchart TB
    source["Source<br/>raw_bts.flights"]

    staging["Staging view<br/>stg_bts__flights"]
    intermediate["Intermediate view<br/>int_flights__quality_classified"]

    subgraph core["Core dimensional layer"]
        dimDate["dim_date"]
        dimCarrier["dim_carrier"]
        dimAirport["dim_airport"]
        dimCause["dim_delay_cause"]
        factFlights["fact_flights"]
        factCause["fact_flight_delay_cause"]
    end

    subgraph marts["Business-facing analytical marts"]
        carrierMart["mart_carrier_monthly_performance"]
        routeMart["mart_route_monthly_performance"]
        airportMart["mart_airport_monthly_performance"]
        causeMart["mart_carrier_monthly_delay_cause"]
    end

    source --> staging
    staging --> intermediate

    intermediate --> dimDate
    intermediate --> dimCarrier
    intermediate --> dimAirport
    intermediate --> factFlights

    dimCause --> factCause
    intermediate --> factCause

    factFlights --> carrierMart
    factFlights --> routeMart
    factFlights --> airportMart

    factCause --> causeMart
    dimCause --> causeMart
```

### Layer definitions

| Layer | Materialization | Role |
|---|---|---|
| Source | BigQuery table | Validated landing data |
| Staging | View | Naming, source flags, anomaly exposure, deterministic flight key |
| Intermediate | View | Central semantic and metric-eligibility classification |
| Core dimensions | Tables or seed | Reusable date, carrier, airport, and delay-cause entities |
| Flight fact | Sandbox view | Flight-grain analytical record |
| Delay-cause fact | Table | Sparse flight-and-positive-cause attribution |
| Analytical marts | Tables | Monthly business-facing aggregates |

## 4. Semantic Classification Boundary

```mermaid
flowchart LR
    sourceRecord["Standardized flight record"]

    classifier["Semantic quality classification"]

    scheduled["Scheduled-flight metrics"]
    operated["Operated-flight metrics"]
    delay["Observed delay metrics"]
    duration["Eligible duration metrics"]
    cause["Delay-cause metrics"]

    sourceRecord --> classifier

    classifier --> scheduled
    classifier --> operated
    classifier --> delay
    classifier --> duration
    classifier --> cause
```

The intermediate layer prevents downstream models from repeatedly interpreting operational edge cases.

Examples include:

- cancelled flights without observed departure or arrival values;
- diverted flights with incomplete destination outcomes;
- null operational durations;
- negative scheduled elapsed time;
- unavailable flight numbers requiring a validated fallback key; and
- delay-cause fields that are incomplete or ineligible for decomposition.

This is a semantic contract: a record can remain preserved in the flight-grain model while being excluded from a particular metric’s denominator.

## 5. Core Dimensional Warehouse
The core dimensional warehouse follows a fact-constellation, also called a galaxy-schema, pattern.

Two fact tables represent related analytical processes:

- `fact_flights` represents the scheduled-flight process.
- `fact_flight_delay_cause` represents positive BTS delay attribution associated with eligible flights.

The facts share conformed date, carrier, and airport dimensions. The sparse delay-cause fact additionally connects to the controlled delay-cause dimension.

```mermaid
flowchart TB
    subgraph topRow[" "]
        direction LR

        topSpacerLeft[" "]
        dimDate["dim_date"]
        topSpacerRight[" "]
    end

    subgraph middleRow[" "]
        direction LR

        factFlights["fact_flights<br/><br/>One scheduled flight"]
        factDelay["fact_flight_delay_cause<br/><br/>Flight + positive cause"]
        dimCause["dim_delay_cause"]
    end

    subgraph bottomRow[" "]
        direction LR

        dimCarrier["dim_carrier"]
        dimAirport["dim_airport<br/>origin / destination"]
        bottomSpacer[" "]
    end

    dimDate --> factFlights
    dimDate --> factDelay

    factFlights --> dimCarrier
    factDelay --> dimCarrier

    factFlights --> dimAirport
    factDelay --> dimAirport

    factDelay --> dimCause

    factFlights -. "flight_key · one-to-many" .-> factDelay

    classDef dimension fill:#D9EAD3,stroke:#274E13,stroke-width:1.5px,color:#000000
    classDef fact fill:#2E8B57,stroke:#145A32,stroke-width:2px,color:#FFFFFF
    classDef ghost fill:transparent,stroke:transparent,color:transparent

    class dimDate,dimCarrier,dimAirport,dimCause dimension
    class factFlights,factDelay fact
    class topSpacerLeft,topSpacerRight,bottomSpacer ghost

    style topRow fill:transparent,stroke:transparent
    style middleRow fill:transparent,stroke:transparent
    style bottomRow fill:transparent,stroke:transparent
```

### Galaxy-schema relationships

| Dimension | `fact_flights` | `fact_flight_delay_cause` | Role |
|---|:---:|:---:|---|
| `dim_date` | Yes | Yes | Shared flight-date calendar |
| `dim_carrier` | Yes | Yes | Shared BTS reporting-carrier identity |
| `dim_airport` | Yes | Yes | Role-playing origin and destination airport |
| `dim_delay_cause` | No | Yes | Controlled category for attributed delay minutes |

### Fact grains

| Fact | Grain | Analytical process |
|---|---|---|
| `fact_flights` | One reported scheduled flight | Flight operations, disruption, reliability, duration, taxi time, airtime, and distance |
| `fact_flight_delay_cause` | One eligible flight and positive delay cause | Sparse attribution of delay minutes across standardized BTS causes |

### Why this is a galaxy schema

A star schema normally centers on one fact table surrounded by dimensions.

This warehouse contains two related facts that reuse several conformed dimensions. That makes it a fact constellation or galaxy schema:

```text
Shared dimensions
       |
       +-- fact_flights
       |
       +-- fact_flight_delay_cause
```

The shared dimensions allow flight operations and delay attribution to be analyzed consistently by:

- date;
- carrier;
- origin airport; and
- destination airport.

`dim_delay_cause` is specialized because it applies only to the delay-attribution process.

A flight can contribute more than one positive delay-cause row. Therefore:

- `delay_minutes` is additive across delay causes;
- delay-cause rows are not distinct delayed flights;
- distinct flight analysis must use `flight_key`; and
- the relationship from `fact_flights` to `fact_flight_delay_cause` is conceptually one-to-many.

## 6. Analytical Mart Relationships

```mermaid
flowchart TB
    flights["fact_flights"]
    causes["fact_flight_delay_cause"]

    carrier["Carrier monthly performance<br/>month + carrier"]
    route["Route monthly performance<br/>month + origin + destination"]
    airport["Airport monthly performance<br/>month + airport"]
    delayCause["Carrier monthly delay cause<br/>month + carrier + cause"]

    powerbi["Power BI semantic and reporting layer"]

    flights --> carrier
    flights --> route
    flights --> airport
    causes --> delayCause

    carrier --> powerbi
    route --> powerbi
    airport --> powerbi
    delayCause --> powerbi
```

### Mart purposes

| Mart | Grain | Primary analytical purpose |
|---|---|---|
| `mart_carrier_monthly_performance` | Month and carrier | Carrier volume, reliability, delay, duration, taxi, and distance performance |
| `mart_route_monthly_performance` | Month, origin, and destination | Route traffic, disruption, reliability, and operating-time performance |
| `mart_airport_monthly_performance` | Month and airport | Inbound and outbound activity, delay, disruption, connectivity, and taxi behavior |
| `mart_carrier_monthly_delay_cause` | Month, carrier, and cause | Attributable delay burden, affected flight-cause records, severity, and cause share |

## 7. Metric Aggregation Design

```mermaid
flowchart LR
    additive["Additive totals<br/>delay minutes · flight counts"]
    denominator["Eligible denominators<br/>observed or qualified flights"]
    calculation["Recalculated metric<br/>sum(total) / sum(eligible count)"]
    report["Correct selected-period result"]

    additive --> calculation
    denominator --> calculation
    calculation --> report
```

The marts preserve additive totals and their eligible denominators so downstream tools can calculate correct results across arbitrary filter selections.

For example:

```text
Selected-period average arrival delay
=
sum(total_arrival_delay_minutes)
/
sum(arrival_observed_flights)
```

This is safer than averaging already-calculated monthly averages.

The same principle applies to:

- departure-delay averages;
- arrival-delay averages;
- scheduled and actual duration averages;
- airtime averages;
- taxi-in and taxi-out averages;
- cancellation rates;
- diversion rates;
- 15-minute delay rates; and
- delay-cause shares.

## 8. Testing and Reconciliation Architecture

```mermaid
flowchart TB
    source["BigQuery landing source"]

    staging["Staging"]
    intermediate["Quality-classified intermediate"]
    facts["Facts and dimensions"]
    marts["Analytical marts"]

    sourceTest["Source-to-staging<br/>row preservation"]
    semanticTest["Operational and metric<br/>eligibility rules"]
    factTest["Fact grain, keys,<br/>relationships, reconciliation"]
    martTest["Mart grain, rates,<br/>coverage, volume reconciliation"]

    source --> staging
    staging --> intermediate
    intermediate --> facts
    facts --> marts

    staging -.-> sourceTest
    intermediate -.-> semanticTest
    facts -.-> factTest
    marts -.-> martTest
```

The dbt project contains:

- 11 models;
- 214 data tests;
- 1 seed;
- 1 source; and
- 26 custom singular tests.

The latest preserved complete build result was:

```text
PASS=226 WARN=0 ERROR=0 SKIP=0 TOTAL=226
```

Testing covers:

- source-to-staging row preservation;
- staging-to-intermediate row preservation;
- deterministic flight-key validity;
- operational classification rules;
- metric-eligibility rules;
- duration eligibility;
- flight-fact row preservation;
- positive and unique delay-cause records;
- dimension-key relationships;
- mart grain uniqueness;
- rate boundaries;
- preservation of carrier, route, and airport month populations;
- fact-to-mart volume reconciliation; and
- delay-cause shares summing to approximately one.

## 9. Sandbox-Aware Physical Design

```mermaid
flowchart LR
    sandbox["BigQuery Sandbox constraints"]

    flightView["fact_flights<br/>view"]
    causeTable["fact_flight_delay_cause<br/>clustered table"]
    martTables["Monthly marts<br/>tables"]
    targeted["Targeted dbt builds"]

    sandbox --> flightView
    sandbox --> causeTable
    sandbox --> martTables
    sandbox --> targeted
```

The current physical design reflects the project environment:

- `fact_flights` remains a view to avoid storing another full copy of the 14.1-million-row flight history;
- `fact_flight_delay_cause` is a non-partitioned clustered table because historical date partitions are subject to Sandbox expiration behavior;
- monthly analytical marts are tables;
- staging and intermediate models are views; and
- targeted builds reduce unnecessary BigQuery processing during development.

A future non-Sandbox production design may introduce:

- an incremental flight fact;
- date partitioning;
- additional clustering;
- source freshness;
- snapshots;
- orchestration;
- continuous integration; and
- controlled deployment environments.

These are future enhancements, not current-state claims.

## 10. Documentation Map

| Document | Purpose |
|---|---|
| `../scripts/README.md` | Python data-preparation workflow |
| `../analysis/profiling/README.md` | Profiling and validation evidence |
| `../dbt/README.md` | dbt models, tests, metrics, and operating guidance |
| `BIGQUERY_RUNBOOK.md` | Ingestion, validation, recovery, and acceptance procedure |
| `ARCHITECTURE.md` | End-to-end architecture and warehouse relationships |

These documents separate implementation guidance, operational procedure, evidence interpretation, and architectural explanation while remaining mutually consistent.