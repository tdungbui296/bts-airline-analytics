# B3.11 — BTS BigQuery load command PREVIEW (NOT AN EXECUTION SCRIPT)

**Status: REVIEW ONLY. DO NOT RUN THESE COMMANDS.**

- Target project: `tdb-dbt-bigquery-portfolio`; region: `asia-southeast1`.
- Source: 24 historical monthly gzip CSV files plus January/February 2026.
- The commands below use append behavior (`--noreplace`); append is NOT idempotent.
- **BLOCKED** until the explicitly authorized billed phase and all remote resource/
  already-loaded-month checks, permission checks, cost/quota safeguards, and
  partition-retention fixes are complete.
- Do not paste individual load commands from this document into a terminal.
- Each completed load needs a job ID, month-specific warehouse count,
  and durable receipt before another month is authorized.
- If a load errors or its receipt is missing, stop; verify the remote table and
  job status before considering a retry. Never blindly append again.
- These are NEW proposed loading commands; they are not historical load logs.

## Proposed command pattern (review only)

```powershell
# 2024-01: 547,271 rows; SHA256 3FEB24F2C484188F35AECAB1BF9B8B3E157577D5BCD6362EFC5D23C734925449
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_01_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-02: 519,221 rows; SHA256 BDF321D56075DB19092E7EBD12384ACCF923D7FF454EE5500C32E8C6B4955D6D
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_02_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-03: 591,767 rows; SHA256 A1AAB2002372A2C96821546B58936715F5BAB135574B75563BF2C03EF41F2B92
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_03_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-04: 582,185 rows; SHA256 3AE9E4FB0CCC5CDD9603BF5F4E2D8A5C64C2929C891EB3A630F643A96A64AB2A
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_04_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-05: 609,743 rows; SHA256 0AD8CA348C93F7D4FF5C8071740108173168940D780506FD9E1CEDCA97411AD5
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_05_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-06: 611,132 rows; SHA256 3F7F3AC67646B5D5C78E874F26F00E7F46B9A11C560C6D1465927B360D4135E8
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_06_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-07: 634,613 rows; SHA256 4C42CC96F58EA580F131CC2F49FA1B9A6FDEC63EC1175002C26375EB33279618
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_07_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-08: 619,025 rows; SHA256 55242F90184FDD5210094CA6A884266098EDEDA649253FA47873610DB26BC743
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_08_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-09: 582,622 rows; SHA256 78A37D5A993B6360412FCE0C4058A4DF67D1454B0AE3ADBC9044A7B0A5AAF57B
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_09_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-10: 615,497 rows; SHA256 6F83B7A1C7767AD6FC10F46F4B4CEC09A10526299B84109AC9700317189FC418
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_10_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-11: 575,404 rows; SHA256 7505084318B4DA869E2D0939F7A98BCA27EFF62AE5FDCFE04ADF517B1FEE6F10
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_11_bq.csv.gz" "config/bq_flights_schema.json"

# 2024-12: 590,581 rows; SHA256 C37532BC51EF80775F1A0A20582532299E10542422A58E0DBD06CE2254BBFCDB
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2024/bts_2024_12_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-01: 539,747 rows; SHA256 5967C50B93041CC7A537B0E2102514922A8535AE55557259472FF5CF4286E5B9
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_01_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-02: 504,884 rows; SHA256 0C7252E714E5E295282275B18ACB8395583A9F64773F24D25FB4D0E0C01D302F
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_02_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-03: 600,872 rows; SHA256 AC050A339797DEE71C4107676411E6C304AF7F5AFDC9E6C3E36A6927F6E880B6
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_03_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-04: 583,950 rows; SHA256 57A9D75BAB9C7A1E8FF86C259CDD8ADF8B4305620BBFCF2A4545BC0827A8B8E7
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_04_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-05: 605,648 rows; SHA256 5FDAA075C9EBD642B1A48FF41D84521D8959521086401248EFFF0AB7DD30B0C2
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_05_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-06: 611,575 rows; SHA256 94DC4AF71A41F272A5ACE756D2829C863FD3120A160707151ACBEF55B37C0E8E
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_06_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-07: 631,428 rows; SHA256 BA0A7F3D6A230C44C9B3C48AFF229729999FC672EDD105ECFC039F4130312BB1
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_07_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-08: 602,378 rows; SHA256 815BF74428AB692B167C9226F8ABFB797646A9940E3D6630C9CE5261D2B99EF5
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_08_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-09: 562,439 rows; SHA256 7F6DFC025AD5A7704CBA265CB67F18B413713356B0C23E8F357908023ED4D49B
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_09_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-10: 605,844 rows; SHA256 6B2A759CFE690E269A59BF475F6512370E92326A0911A7450CEB0BE0291BA114
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_10_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-11: 570,550 rows; SHA256 1C371E662985B6DF22013EE268CD694A68D00EE0920FCF084CFFD0AA1608CF21
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_11_bq.csv.gz" "config/bq_flights_schema.json"

# 2025-12: 582,304 rows; SHA256 F8954F57893775766CE3ACD0EA972D68F5FA2C53497E867FC0301ECFD8930135
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights" "data/load/2025/bts_2025_12_bq.csv.gz" "config/bq_flights_schema.json"

# 2026-01: 544,003 rows; SHA256 41236900C1E7311045C4B020CC7E67E35EDE4143E4B48325CB1201C725F33849
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights_2026" "data/load/2026/bts_2026_01_bq.csv.gz" "config/bq_flights_schema.json"

# 2026-02: 515,037 rows; SHA256 4DAA3CC9C55AE54739E878B41300E0BA2AE79B141A02DE339B312877F963AC7C
# DO NOT RUN until remote month absence is independently established.
bq --location=asia-southeast1 load --source_format=CSV --skip_leading_rows=1 --max_bad_records=0 --noreplace "tdb-dbt-bigquery-portfolio:raw_bts.flights_2026" "data/load/2026/bts_2026_02_bq.csv.gz" "config/bq_flights_schema.json"

```

## Required deployment gates (not satisfied in this preview)

1. Explicit authorization to attach billing/trial; verify project, quotas, budgets, and permissions.
2. Remove `dbt_dev` default partition expiration, keep controlled table TTL, and verify metadata.
3. Confirm no prior month is loaded before any append; record job IDs and monthly counts.
4. Restore historical landing data; build historical views and bootstrap the physical incremental table.
5. Confirm partition and clustering metadata; load January then February individually.
6. Run month-scoped MERGE and replay/correction tests with separate evidence and cost checks.
7. Maintain separate raw-load idempotency checks: a dbt MERGE does not prevent CSV append duplicates.
