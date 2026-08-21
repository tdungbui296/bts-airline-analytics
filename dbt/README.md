# BTS Airline Analytics — dbt Warehouse

This dbt project transforms validated US Bureau of Transportation Statistics Reporting Carrier On-Time Performance data into a tested dimensional warehouse and a set of business-facing analytical marts in Google BigQuery.

The modeled historical dataset covers reporting years 2024 and 2025:

| Year | Source files | Flight records |
|---:|---:|---:|
| 2024 | 12 monthly files | 7,079,061 |
| 2025 | 12 monthly files | 7,001,619 |
| Total | 24 monthly files | 14,080,680 |

The project demonstrates analytics-engineering practices including layered SQL transformation, dimensional modeling, semantic metric eligibility, data-quality testing, source-to-mart reconciliation, and BigQuery-aware materialization decisions.

## Technology

- dbt Core 1.12.0
- dbt BigQuery 1.12.0
- Google BigQuery
- Google Cloud project: `tdb-dbt-bigquery-portfolio`
- BigQuery location: `asia-southeast1`
- Local authentication: Google OAuth
- Python 3.12.10

The repository-level Python environment and installation procedure are documented in `../scripts/README.md`.

## Project Structure

```text
dbt/
├── analyses/
├── macros/
├── models/
│   ├── staging/
│   │   └── bts/
│   │       ├── _bts__sources.yml
│   │       ├── _bts__models.yml
│   │       └── stg_bts__flights.sql
│   ├── intermediate/
│   │   └── flights/
│   │       ├── _flights__models.yml
│   │       └── int_flights__quality_classified.sql
│   └── marts/
│       ├── core/
│       │   ├── _core__models.yml
│       │   ├── dim_airport.sql
│       │   ├── dim_carrier.sql
│       │   ├── dim_date.sql
│       │   ├── fact_flight_delay_cause.sql
│       │   └── fact_flights.sql
│       └── analytics/
│           ├── _analytics__models.yml
│           ├── mart_airport_monthly_performance.sql
│           ├── mart_carrier_monthly_delay_cause.sql
│           ├── mart_carrier_monthly_performance.sql
│           └── mart_route_monthly_performance.sql
├── seeds/
│   ├── _seeds.yml
│   └── dim_delay_cause.csv
├── tests/
├── dbt_project.yml
└── README.md
```

## Transformation Architecture

The project follows four transformation layers:

```text
raw_bts.flights
        |
        v
stg_bts__flights
        |
        v
int_flights__quality_classified
        |
        v
dimensions and flight-grain facts
        |
        v
monthly analytical marts
```

### Source layer

The declared source is:

- `raw_bts.flights`

It contains validated flight-level records loaded from 24 monthly BTS source files covering 2024 and 2025.

Raw acquisition, local validation, projection, type auditing, and BigQuery load-file generation occur outside dbt and are documented in `../scripts/README.md`.

### Staging layer

#### `stg_bts__flights`

Materialization: view

Grain: one row per reported scheduled flight

This model:

- standardizes source column names;
- converts source indicators into analyst-friendly Boolean fields;
- preserves operational source values for traceability;
- identifies known source-data conditions;
- creates a deterministic SHA-256 flight key;
- applies a validated fallback key strategy when the reporting flight number is unavailable; and
- preserves source row volume.

### Intermediate layer

#### `int_flights__quality_classified`

Materialization: view

Grain: one row per reported scheduled flight

This model centralizes semantic quality rules and determines whether each record is eligible for specific analytical metrics.

The classifications distinguish concepts such as:

- scheduled flights;
- operationally cancelled flights;
- operated flights;
- observed departure and arrival delays;
- valid scheduled and actual flight durations;
- distance and airtime observations; and
- usable BTS delay-cause attribution.

Centralizing these rules prevents each downstream mart from independently interpreting nulls, cancellations, diversions, malformed durations, and incomplete delay-cause fields.

## Dimensional Warehouse

### Dimensions

| Model | Materialization | Grain | Purpose |
|---|---|---|---|
| `dim_date` | Table | One row per calendar date | Conformed calendar attributes spanning the modeled flight dates |
| `dim_carrier` | Table | One row per BTS reporting-airline DOT ID | Stable reporting-carrier identity |
| `dim_airport` | Table | One row per stable BTS airport ID | Reusable role-playing airport dimension for origins and destinations |
| `dim_delay_cause` | Seed | One row per standardized delay cause | Controlled mapping of the five supported BTS delay causes |

The standardized delay causes are:

- Carrier
- Weather
- National Airspace
- Security
- Late Aircraft

### Flight fact

#### `fact_flights`

Current materialization: view

Grain: one row per reported scheduled flight

The fact exposes:

- conformed date, carrier, origin, and destination keys;
- operational status measures;
- departure and arrival delay measures;
- elapsed-time, airtime, taxi-time, and distance measures;
- BTS delay-cause fields; and
- explicit metric-eligibility flags inherited from the intermediate layer.

During BigQuery Sandbox development, this model is intentionally materialized as a view to avoid duplicating the full 14.1-million-row historical flight dataset.

The intended production evolution is a partitioned and clustered incremental table once billing, storage retention, and orchestration requirements justify that design.

### Delay-cause fact

#### `fact_flight_delay_cause`

Materialization: table

Grain: one row per eligible flight and positive standardized delay cause

This sparse fact converts the five BTS delay-cause columns into an analytical row-based structure. Null and zero-minute causes are excluded.

A delayed flight may contribute rows to multiple delay causes. Therefore:

- `delay_minutes` is additive across causes;
- flight-cause row counts are not equivalent to unique delayed-flight counts; and
- unique flight counts must use `flight_key`.

In the current Sandbox design, the model is non-partitioned because historical date partitions are subject to Sandbox expiration constraints.

## Analytical Marts

### `mart_carrier_monthly_performance`

Grain: one row per flight month and reporting carrier

Purpose:

- compare carrier operating volumes;
- measure cancellations and diversions;
- calculate departure and arrival reliability;
- summarize delays, flight durations, airtime, taxi time, and distance; and
- provide additive totals and their corresponding eligible-flight denominators.

### `mart_route_monthly_performance`

Grain: one row per flight month, origin airport, and destination airport

Purpose:

- compare route-level traffic and reliability;
- measure cancellations and diversions;
- evaluate departure and arrival delays;
- analyze scheduled and actual flight duration;
- summarize airtime, taxi time, and distance; and
- preserve route-month coverage from the flight fact.

### `mart_airport_monthly_performance`

Grain: one row per flight month and airport

Purpose:

- compare airport departure and arrival activity;
- measure departure cancellations;
- evaluate outbound and inbound delay performance;
- summarize taxi-out and taxi-in behavior;
- count serving carriers, destinations, and origins; and
- distinguish scheduled inbound diversions from physical diverted-aircraft arrivals.

### `mart_carrier_monthly_delay_cause`

Grain: one row per flight month, reporting carrier, and standardized delay cause

Purpose:

- quantify affected flight-cause records;
- aggregate attributable delay minutes;
- compare cause severity;
- calculate each cause’s share of a carrier-month’s attributed delay burden; and
- reconcile analytical cause totals back to `fact_flight_delay_cause`.

Because one flight can have multiple positive causes, `affected_flights` must not be summed across delay causes and interpreted as a distinct flight count.

## Materializations

The default materializations configured in `dbt_project.yml` are:

| Layer | Default materialization |
|---|---|
| Staging | View |
| Intermediate | View |
| Marts | Table |

`fact_flights` overrides the marts default and remains a view during Sandbox development.

This split keeps reusable transformation logic lightweight while physically materializing smaller conformed dimensions, the sparse delay-cause fact, and business-facing aggregate marts.

## Data Quality

The current project contains:

- 11 models;
- 1 seed;
- 1 declared source;
- 214 data tests; and
- 26 custom singular tests.

The latest complete build result was:

```text
PASS=226 WARN=0 ERROR=0 SKIP=0 TOTAL=226
```

### Generic tests

Model and seed YAML files define tests such as:

- `not_null`;
- `unique`;
- `accepted_values`; and
- `relationships`.

These validate primary keys, foreign keys, controlled domains, and required analytical measures.

### Singular tests

Custom SQL tests cover higher-order warehouse invariants, including:

- staging and intermediate row-count preservation;
- deterministic flight-key validity;
- operational and metric-eligibility rules;
- duration eligibility;
- fact row-count preservation;
- positive delay-cause minutes;
- sparse fact grain;
- preservation of eligible positive cause rows;
- analytical mart uniqueness;
- valid rate boundaries;
- source-to-mart volume reconciliation;
- preservation of carrier-month, route-month, and airport-month populations;
- reconciliation of delay-cause marts to their underlying fact; and
- delay-cause shares summing to approximately one.

A test passes when its SQL query returns zero violating rows.

## Metric Interpretation

The analytical marts preserve additive numerators and eligible-record denominators wherever practical.

For example, a selected-period average should generally be recalculated as:

```text
sum(total_metric) / sum(eligible_record_count)
```

Averages and rates should not ordinarily be averaged directly across carriers, routes, airports, or months.

Examples include:

- average delay from total delay minutes divided by observed flights;
- cancellation rate from cancelled flights divided by scheduled flights;
- diversion rate from diverted flights divided by scheduled flights;
- average airtime from total airtime divided by airtime-observed flights; and
- selected-period delay-cause share from cause delay minutes divided by total attributable delay minutes.

This design makes the marts safer for Power BI and other downstream reporting tools.

## Local Configuration

The dbt profile is intentionally excluded from Git.

Create the local profile at:

```text
%USERPROFILE%\.dbt\profiles.yml
```

The profile name must match:

```text
bts_airline_analytics
```

A structural example is shown below. Replace placeholder values with the appropriate local configuration.

```yaml
bts_airline_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: your-gcp-project-id
      dataset: dbt_dev
      location: asia-southeast1
      threads: 4
```

Do not commit `profiles.yml`, OAuth tokens, service-account files, private keys, or other credentials.

## Running the Project

From the repository root, activate the project environment:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

Confirm the dbt installation:

```powershell
dbt --version
```

Parse the project without querying or modifying warehouse relations:

```powershell
dbt parse `
    --project-dir .\dbt `
    --no-partial-parse
```

List project resources:

```powershell
dbt ls `
    --project-dir .\dbt
```

Load the delay-cause seed:

```powershell
dbt seed `
    --project-dir .\dbt
```

Build and test the complete project:

```powershell
dbt build `
    --project-dir .\dbt
```

Build a single model and its attached tests:

```powershell
dbt build `
    --project-dir .\dbt `
    --select mart_route_monthly_performance
```

Build a model together with its upstream dependencies:

```powershell
dbt build `
    --project-dir .\dbt `
    --select +mart_carrier_monthly_performance
```

Generate dbt documentation:

```powershell
dbt docs generate `
    --project-dir .\dbt
```

Serve the generated documentation locally:

```powershell
dbt docs serve `
    --project-dir .\dbt
```

## Cost and Sandbox Considerations

This project was developed using BigQuery Sandbox, so warehouse design decisions account for limited storage and historical partition expiration.

Before running a full build:

- confirm the active GCP project and target dataset;
- review which models will be rebuilt;
- consider BigQuery bytes processed;
- use targeted model selection during development; and
- avoid repeatedly rebuilding large flight-grain relations unnecessarily.

`dbt parse` is the safest first validation because it checks project syntax and dependency resolution without building warehouse relations.

The four monthly analytical marts are physical tables and therefore incur query and storage work when rebuilt.

## Scope

The current repository demonstrates a complete batch analytics-engineering workflow for the 2024–2025 historical dataset.

Potential later enhancements include:

- incremental materialization of the flight fact;
- partitioning and clustering under a non-Sandbox retention model;
- dbt snapshots for changing reference attributes;
- automated orchestration;
- continuous integration;
- source-freshness monitoring; and
- a 2026 holdout-data refresh.

These are intentionally treated as future enhancements rather than presented as already implemented.