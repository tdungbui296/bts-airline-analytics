# BTS Airline Analytics Engineering — B3.10 Retention and Cost Gate

**Status:** DEPLOYMENT PLAN ONLY. Billing, dataset settings, loads, dbt runs, and BigQuery DML have **not** been changed or executed by this checkpoint.

**Project:** `tdb-dbt-bigquery-portfolio`  
**BigQuery location:** `asia-southeast1`  
**Verified local files:** 26 BigQuery-ready CSV.GZ files with ordered 51-column headers and manifest SHA-256 matches. 2024: 7,079,061 rows (12 months); 2025: 7,001,619 (12); January 2026: 544,003; February 2026: 515,037. Combined expected source rows: 15,139,720.

## Critical B3.9 finding

Both existing datasets (`raw_bts`, `dbt_dev`) are located in `asia-southeast1`, currently have no tables/views, and declare **60-day default table expiration** plus **60-day default partition expiration**. BigQuery computes date-partition expiration based on the partition's date boundary, not the date of loading. Creating the historical `flight_date`-partitioned table under that default would put historical partitions outside their retention window immediately. This is a **deployment blocker**.

## Agreed lifecycle policy (to apply only in the user-approved billed execution phase)

1. Before recreating tables or issuing dbt bootstrap, remove the **dataset-level default partition expiration** on `dbt_dev`, where the incremental fact will be materialized. The `raw_bts` dataset may also have its partition expiration removed if a future partitioned landing table is planned; for the current **unpartitioned** 51-column landing tables, the dataset's default table lifetime is the controlling setting.
2. **Retain the 60-day default table expiration** as a cleanup policy for newly created objects in both datasets, unless a later explicit retention decision is made. This keeps the portfolio demo transient while protecting old **flight-date** partitions from age-based deletion.
3. After the dataset update, read its JSON metadata and require `defaultPartitionExpirationMs` to be absent or zero **before** any partitioned fact bootstrap.
4. Immediately after bootstrap, read the incremental **table** metadata and verify partition field `flight_date`, clustering, partition expiration absent, and appropriate table expiration. Reject if partition expiration is inherited or the table is not the expected physical partitioned table.
5. In Sandbox mode, keep current settings unchanged. Sandbox itself imposes 60-day expiry and disallows DML; do not claim changing one dataset setting removes those Sandbox product restrictions.

**Documentation for later (DO NOT RUN NOW):**

```powershell
# ONLY after explicit authorization to attach billing, enable an eligible trial,
# and configure quota/budget controls, and BEFORE creating the partitioned fact:
$Project = 'tdb-dbt-bigquery-portfolio'
bq update --default_partition_expiration=0 "${Project}:dbt_dev"
if ($LASTEXITCODE -ne 0) { throw 'Partition-expiration update failed' }

$Info = (bq show --format=json "${Project}:dbt_dev" | Out-String | ConvertFrom-Json)
if ($LASTEXITCODE -ne 0) { throw 'Unable to verify dbt_dev metadata' }
if ($Info.defaultPartitionExpirationMs -and
    [long]$Info.defaultPartitionExpirationMs -ne 0) {
    throw 'BLOCKED: default partition expiration still configured'
}
Write-Host 'Verified: dbt_dev has no default partition expiration'
```

Do not blindly run this excerpt from the runbook; the actual deployment script should first reconfirm identity, region, current project, pricing/quotas, and safe resource state. `bq update` changes cloud metadata and is NOT a read-only command.

## Cost-control gates (not yet completed)

- Confirm **explicit user decision** for any billing or Free Trial attachment, eligibility, and current billing status via the Google Cloud UI; never paste billing account numbers, credentials, or tokens in ChatGPT or commit them.
- Configure **custom project or user daily BigQuery query quotas**. Budget alerts are useful notifications, **not** hard caps. Account for storage and load jobs separately from on-demand query quotas.
- Estimate or dry-run BigQuery **query** operations once the restored tables exist; verify maximum bytes billed for supported query jobs. A local dbt parse or Jinja mock is **not** a BigQuery cost estimate.
- Limit cloud operations to a reviewed, manually authorized set. Confirm the target project, region, source schemas, and destination table names before each operation.
- Capture job IDs, job configuration, actual bytes, rows, and costs for recruiter evidence without exposing secrets.

## Execution sequence (future, gated)

1. Apply and verify safe partition-retention settings after billing is authorized, **before any partitioned tables are created**.
2. Restore 24 historical months into `raw_bts.flights` from verified files exactly once. Use the explicit 51-column JSON schema, CSV header skip, bad-record rejection, and region; record actual 2024/2025 counts and job receipts. Avoid blind `WRITE_APPEND` retries.
3. Recreate historical dbt views and bootstrap the physical `fact_flights_incremental_demo` with 14,080,680 unique flight keys; verify table and partition metadata and no loss of historical partitions.
4. Load January 2026 into dedicated landing `raw_bts.flights_2026`, build gated 2026 views, then execute January monthly MERGE. Confirm 544,003 keys processed and cumulative fact count independently.
5. Replay January **without duplicating the raw load**; verify target key/count/hash stability.
6. Add February to landing once and MERGE February; verify 515,037 monthly keys and cumulative counts.
7. Test a **non-key** correction on a controlled fixture, followed by restoration; note that identity-changing corrections need a different matching strategy than current `flight_key`.
8. Run dbt and SQL reconciliation tests, collect compilation/BigQuery execution metrics, export evidence, and preserve the historical Power BI baseline.

## Evidence distinctions

- **Verified:** Original source integrity, processed and normalized January/February files, local row counts/hashes, all 26 local load-file headers, B3.5 offline mock checks, BigQuery dataset metadata/empty inventory.
- **Not yet verified:** Native warehouse-backed dbt compilation with these tables, 2024–2026 restored physical warehouse, actual `MERGE`, correction/replay behavior in BigQuery, partition-pruning and cost metrics.

## References

- Google Cloud, Update dataset properties: https://docs.cloud.google.com/bigquery/docs/updating-datasets
- Google Cloud, Manage partitioned tables: https://docs.cloud.google.com/bigquery/docs/managing-partitioned-tables
- Google Cloud, BigQuery Sandbox: https://docs.cloud.google.com/bigquery/docs/sandbox
- Google Cloud, Estimate and control costs: https://docs.cloud.google.com/bigquery/docs/best-practices-costs
- dbt BigQuery configurations: https://docs.getdbt.com/reference/resource-configs/bigquery-configs
