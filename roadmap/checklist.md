**Handoff Checklist**

**Completed**
- [x] Phase 1: Historical REST batch ingestion implemented in [batch_ingest.py](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/src/ingestion/batch_ingest.py)
- [x] Batch ingest supports Binance klines pull, normalization, retry/backoff, resume state, partitioned output, optional S3 upload
- [x] Batch ingest writes Parquet locally
- [x] Batch ingest S3 upload path is wired
- [x] Batch ingest workflow smoke test exists in [test_batch_ingest_workflow.py](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/scripts/test_batch_ingest_workflow.py)
- [x] Batch ingest tests were run successfully

- [x] Phase 2: Schema contract created in [kline_1m.schema.json](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/schemas/kline_1m.schema.json)
- [x] ClickHouse DDL scaffold created under [sql/clickhouse](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/sql/clickhouse)
- [x] Local dev stack scaffolded in [docker-compose.yml](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/docker-compose.yml)
- [x] ClickHouse, Grafana, and Spark UI were brought up successfully

- [x] Phase 3: WebSocket stream producer implemented in [stream_producer.py](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/src/ingestion/stream_producer.py)
- [x] Producer filters for closed candles only
- [x] Producer validates against schema-required fields
- [x] Producer supports dry-run and real Kinesis publish
- [x] Stream producer smoke test exists in [test_stream_producer_workflow.py](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/scripts/test_stream_producer_workflow.py)
- [x] Stream producer live publish test has worked

**Partially Completed / Needs Confirmation**
- [ ] Phase 0: Confirm AWS Kinesis Data Stream exists and is active in the intended region/account
- [ ] Phase 0: Confirm Firehose delivery stream is created and writing stream records to S3 Bronze
- [ ] Phase 0: Confirm IAM permissions are correct for Kinesis, Firehose, and S3
- [ ] Phase 0: Confirm S3 Bronze/Silver prefix layout matches roadmap
- [ ] Phase 2: Confirm Glue Schema Registry is actually set up in AWS
- [ ] Phase 2: Confirm producer is integrated with Glue Schema Registry, not just local schema validation

**Not Started**
- [x] Phase 4: `stream_to_silver.py` Spark Structured Streaming job
- [ ] Phase 4: Read from Kinesis with Spark
- [ ] Phase 4: Parse/validate/drop malformed rows
- [ ] Phase 4: Deduplicate with watermark
- [ ] Phase 4: Write Silver rows to ClickHouse
- [ ] Phase 4: Write cleaned Parquet to S3 Silver
- [ ] Phase 4: Configure checkpointing to S3

- [x] Phase 5: ClickHouse materialized views for returns, MA, volatility
- [x] Phase 6: EMR Serverless batch jobs
- [x] Phase 7: Airflow DAGs
- [ ] Phase 8: Data quality checks
- [ ] Phase 9: Recovery/backfill workflow
- [ ] Phase 10: Monitoring and observability

**Recommended Next Task For Another Agent**
- [ ] Build [stream_to_silver.py](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/src/jobs/stream_to_silver.py) for Phase 4 first

If you want, I can also rewrite this into a cleaner “agent handoff note” with current status, known pitfalls, and exact next steps.
