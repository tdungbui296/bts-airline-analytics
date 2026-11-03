# BTS 2026 monthly automation — pre-March checkpoint

Checkpoint: B3.51DN | 2026-11-03

## Verified state

- Unified Airflow February fixture: passed (parent and three children).
- Monthly ingestion and dbt fixtures: passed without writes.
- January-February raw reconciliation: 1,059,040 rows.
- March source: 612,102 rows; acquisition verified, not ingested.
- March expected cumulative raw rows after ingestion: 1,671,142.
- Readiness query cap: 200,000,000 bytes per query.
- dbt BigQuery query cap: 1,000,000,000 bytes per query.
- MERGE attempt guard: installed; synthetic recovery tests passed.
- Live March append, MERGE, and reporting: NOT EXECUTED.

## Pipeline source SHA256

- `airflow/compose.monthly.yaml`: `17959D40EFFFB58B783CA45C9C97E53BAF9327A1549B9CD6B9D053DFD67F5B5C`
- `airflow/dags/bts_2026_monthly_prepare.py`: `574D3892CE0EA1AA571718AED3FF4B9E625FF4B89AFD8165895C50EF0A2CC15A`
- `airflow/dags/bts_2026_monthly_ingest.py`: `EBF83D9BE1937F2D07FA13224BA9B2BB5AEF93A4FB5EC97FC0C4D74EEE245248`
- `airflow/dags/bts_monthly_readiness_gate.py`: `4F238B77CD63619BE8346659F3F51A7E5F0CC9EA4E404798CFD60C22A62F8558`
- `airflow/dags/bts_2026_monthly_dbt.py`: `7E5C33DA5F3564CCB5B5A44843D2E4C37DF1108081516E44F29CEE1B15AF9294`
- `airflow/dags/bts_2026_monthly_unified.py`: `FF34CA104DF772E9C8454356D75A11AC199B0B1C2C722434FE4C197819927877`
- `scripts/fetch_2026_bts_source.py`: `A8681D2A87EA232AA09243DB6E9C7B54E74CAEB883F312764E584E91E42CD879`
- `scripts/run_2026_monthly_ingestion.py`: `17397D491CA0C3E1FB9155FDB3BD424F0AB1C5E17F1BE12A1EF95BFEDEFCD530`
- `scripts/build_2026_bq_monthly.py`: `E3673D701DD78B771D407EDE39765AFE565A6A40094B6D4AE273B7B5FC4C8A73`
- `scripts/load_2026_bq_monthly.py`: `BD0C20F969024B11E2EF5EC3C88DF5462753596A7E176C8C66A3CDF4371CA696`
- `scripts/verify_2026_raw_months.py`: `B83748B03F603572EDD751F7D6CC04F7C9D90DEAEB3DF75C03192917A443E0B6`
- `scripts/plan_2026_monthly_dbt.py`: `8296733EB7B4E86B45AE8F6A1B609C61E66EF1D5A2E78DC9DBF0612BB9B1852A`
- `scripts/guard_2026_monthly_merge.py`: `521BB2448BF65D8C0D3E95BEEF5F5C4B4511C13ECB0FA2305568C8D029C19AED`
- `scripts/project_batch.py`: `B7F179A3C72008096B2F5311CDF02B147D1BFA01E19267FAE190BE51A5340C39`
- `dbt/models/incremental_demo/fact_flights_2026_monthly_incremental.sql`: `9FFAEC0B6F6B8B328503CD1A8174DC76D52E9FEE0700E73AB790E816EF3F39FB`
- `evidence/source/2026_monthly_source_manifest.csv`: `7F046F82F78B74BCA606BC79DB271BAD1FE049760A52E6B47452AAD95D426B3C`

## Locally preserved execution logs

These original logs are not included in this commit.
Review and sanitize them before publication.

- `evidence/airflow/B3_51CP_february_fixture.log`: `1B1927F13FDC75F0CAD8026C73311FFB75D73CADF8247B8663836C6D5D4ACD62`
- `evidence/airflow/B3_51CT_ingestion_fixture.log`: `C1B3F94DF0AACF425650142292DF7E355DE23865D30702D518E664414CE7E4B4`
- `evidence/airflow/B3_51DB_dbt_february_fixture.log`: `473B45F72B3A0D6D4F918E37FE444E01F2D5E29B6EE268E3ECD42F9046787C1B`
- `evidence/airflow/B3_51DI_unified_february_fixture.log`: `4ABB549CC65E93B07E3B868A072D08A95727DE0FA97C74D9504DBB1D358FAB8F`
- `evidence/bigquery/B3_51CW_february_raw_reconciliation.log`: `000D877111B5E4A17896244B1B9B12E55BE220AE1D9E7E048EC514B52A2F9770`
- `evidence/bigquery/B3_51DL_march_preflight.log`: `FAD6A6ED2C1E82237514CBDDE6F94AF74305C5DD0D56B34B676C77751A3A8091`

## March source provenance

- Original ZIP SHA256: `C0864D5DABD522517976E29BB81F0C094D114E3E0E40CD51BC5A238AE4292B9D`
- Original ZIP intentionally excluded from Git.

## Remaining limitations

- Live March preparation, append, readiness, and MERGE remain untested together.
- A 1 GB per-query limit is not a cumulative pipeline cost cap.
- Uncertain ingestion or MERGE outcomes require manual reconciliation.
- Unattended monthly scheduling and reporting publication are not enabled.
