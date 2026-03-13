# Processing Modes

The pipeline supports three open-source processing modes plus a premium cloud mode.
Each mode defines where raw data is stored, how transformations run, and where clean
output lands.

## Mode Summary

| Mode                 | Raw data | Transform    | Clean output | Runs where    |
|----------------------|----------|--------------|--------------|---------------|
| `local_sqlite`       | SQLite   | Python/Pandas| SQLite       | Your machine  |
| `local_bq_buffered`  | SQLite   | Python/Pandas| BigQuery     | Your machine  |
| `local_bq_streaming` | Memory   | Pure Python  | BigQuery     | Your machine  |
| `gcp_bq` *(Premium)* | BigQuery | BigQuery SQL | BigQuery     | GCP Cloud Run |

> **Note:** The `gcp_bq` mode is part of the enterprise managed implementation.
> See the [README](../README.md#enterprise-managed-cloud-pipeline) for details.

## Configuration

### YAML (config.enc.yaml)

```yaml
pipeline:
  processing_mode: local_sqlite  # local_sqlite | local_bq_buffered | local_bq_streaming
```

### Environment variable

```bash
export PROCESSING_MODE=local_bq_streaming
```

### CLI override (where supported)

```bash
python scripts/run_pipeline.py --processing-mode local_bq_buffered
```

## Mode Details

### local_sqlite (default)

The simplest mode. Everything runs locally using SQLite. No cloud account required.

- **Best for**: Development, testing, small datasets, offline analysis, Excel reporting
- **Requirements**: None beyond Python and SQLite
- **Data flow**: Ingestion → SQLite `raw_bot_requests` → Python/Pandas transform → SQLite `bot_requests_daily`

```bash
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

### local_bq_buffered

Transforms data locally, then pushes clean records to BigQuery. Raw data is buffered
in SQLite first, then the transform output is pushed to BigQuery.

- **Best for**: Moderate datasets where you want local control but BigQuery as the destination
- **Requirements**: GCP project + BigQuery credentials
- **Data flow**: Ingestion → SQLite `raw_bot_requests` → Python/Pandas transform → BigQuery `bot_requests_daily`

```bash
export PROCESSING_MODE=local_bq_buffered
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

### local_bq_streaming

Pure Python in-memory streaming — no SQLite involved. Records are transformed and
inserted directly into BigQuery in batches.

- **Best for**: Lightweight deployments, minimizing disk I/O, pushing to BigQuery directly
- **Requirements**: GCP project + BigQuery credentials
- **Data flow**: IngestionRecord iterator → PythonTransformer (in-memory) → BigQuery `bot_requests_daily`

```bash
export PROCESSING_MODE=local_bq_streaming
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

## Pipeline Router

The unified `run_pipeline()` function in `pipeline/router.py` reads the processing mode
and dispatches to the correct pipeline:

```python
from llm_bot_pipeline.pipeline import run_pipeline

result = run_pipeline(
    start_date=date(2026, 1, 1),
    end_date=date(2026, 1, 31),
    mode="local_sqlite",  # or omit to use config
)
```

For `local_bq_streaming`, you can also provide an `IngestionRecord` iterator directly:

```python
result = run_pipeline(
    start_date=date(2026, 1, 1),
    end_date=date(2026, 1, 1),
    mode="local_bq_streaming",
    records=my_adapter.stream_records(),
)
```

## Validation

Settings validation automatically checks that required configuration is present for each
mode:

- `local_bq_buffered`, `local_bq_streaming`: require `gcp_project_id`
- `local_sqlite`, `local_bq_buffered`: use SQLite (configured via `sqlite_db_path`)

## Enterprise Cloud Mode

The `gcp_bq` mode runs the full pipeline on GCP Cloud Run Jobs with BigQuery as both
raw and clean storage. This eliminates local compute entirely and supports automated
daily scheduling via Cloud Scheduler.

[Contact us](https://conversem.com/contact/) for enterprise implementation.
