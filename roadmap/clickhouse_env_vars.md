## Henry Shared ClickHouse

Base host details for the teammate-managed ClickHouse server:

```env
CLICKHOUSE_HOST=3.234.211.100
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

Existing teammate database currently in use:

```env
CLICKHOUSE_DB=cleaned_crypto
CLICKHOUSE_RAW_OHLCV_TABLE=ohlcv_1min
CLICKHOUSE_LOAD_MODE=append
S3_CLEANED_BUCKET=is459-crypto-raw-data
```

## KJ Pipeline Shared Database

Phase 1 integration keeps Henry's server but provisions a separate database for this pipeline so no teammate tables are modified.

```env
SHARED_CLICKHOUSE_HOST=3.234.211.100
SHARED_CLICKHOUSE_PORT=8123
SHARED_CLICKHOUSE_DATABASE=is459_streaming_kj
SHARED_CLICKHOUSE_USER=default
SHARED_CLICKHOUSE_PASSWORD=
```

Runtime values that should point at the dedicated shared database:

```env
AIRFLOW_CLICKHOUSE_URL=http://3.234.211.100:8123
AIRFLOW_CLICKHOUSE_VALIDATION_URL=http://3.234.211.100:8123
AIRFLOW_CLICKHOUSE_DATABASE=is459_streaming_kj

STREAM_CLICKHOUSE_URL=http://3.234.211.100:8123
STREAM_CLICKHOUSE_DATABASE=is459_streaming_kj
```

Provisioning entry point:

```powershell
python scripts/provision_shared_clickhouse.py
```

Verification queries:

```sql
SHOW DATABASES;
SHOW TABLES FROM is459_streaming_kj;
```
