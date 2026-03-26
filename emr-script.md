- Republish the artifacts again:

```
python scripts/publish_emr_batch_artifacts.py
```

- Run spark

```
python scripts/submit_emr_serverless_job.py `
  --application-id 00g4bg9qigs98e09 `
  --execution-role-arn arn:aws:iam::050752609589:role/is459-crypto-dev-emr-serverless-exec-role `
  --job-name kaggle-csv-to-bronze-btcusdt `
  --entry-point s3://is459-crypto-datalake/artifacts/emr/phase6/jobs/kaggle_csv_to_bronze.py `
  --py-files s3://is459-crypto-datalake/artifacts/emr/phase6/packages/src.zip `
  --log-uri s3://is459-crypto-datalake/airflow/logs/ `
  --spark-submit-parameters "--conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=1g" `
  --entry-point-argument=--input-path `
  --entry-point-argument=s3://is459-crypto-datalake/bronze/kaggle/btc-price-1m/BTCUSDT/BTCUSDT.csv `
  --entry-point-argument=--output-root `
  --entry-point-argument=s3://is459-crypto-datalake/bronze/rest/kaggle-seed `
  --entry-point-argument=--symbol `
  --entry-point-argument=BTCUSDT

```

- check emr job

```
aws emr-serverless get-job-run `
  --application-id 00g4bg9qigs98e09 `
  --job-run-id <new-job-run-id>

```