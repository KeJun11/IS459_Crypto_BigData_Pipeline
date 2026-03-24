# IS459_Crypto_BigData_Pipeline

Initial scaffold for the hybrid batch and streaming crypto pipeline.

## Step 1: Data Contracts

Schema contract:
- [schemas/kline_1m.schema.json](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\schemas\kline_1m.schema.json)
- [schemas/kline_1m.example.json](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\schemas\kline_1m.example.json)

ClickHouse DDL:
- [00_create_database.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\00_create_database.sql)
- [01_raw_ohlcv_1m.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\01_raw_ohlcv_1m.sql)
- [02_agg_ohlcv_1h.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\02_agg_ohlcv_1h.sql)
- [03_agg_ohlcv_1d.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\03_agg_ohlcv_1d.sql)
- [04_pipeline_metrics.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\04_pipeline_metrics.sql)
- [05_anomaly_events.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\05_anomaly_events.sql)
- [06_technical_features.sql](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\sql\clickhouse\06_technical_features.sql)

## Step 2: Local Dev Stack

The local stack lives in [docker-compose.yml](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\docker-compose.yml) and starts:
- ClickHouse on `8123` and `9000`
- Spark master on `7077` and `8080`
- Spark worker on `8081`
- Grafana on `3000`

Supporting config:
- [spark-defaults.conf](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\infra\docker\spark\conf\spark-defaults.conf)
- [clickhouse datasource](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\infra\docker\grafana\provisioning\datasources\clickhouse.yml)
- [grafana dashboards provider](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\infra\docker\grafana\provisioning\dashboards\dashboards.yml)

Run locally:

```powershell
docker compose up -d
docker compose ps
```

Useful endpoints:
- Spark UI: `http://localhost:8080`
- ClickHouse HTTP: `http://localhost:8123`
- Grafana: `http://localhost:3000` with `admin` / `admin`
- Airflow UI: `http://localhost:8088` with `admin` / `admin` after Airflow is started

## Next

After this, the next practical step is implementing:
- `src/ingestion/batch_ingest.py`
- `src/ingestion/stream_producer.py`

## Terraform AWS Foundation

Terraform for the AWS foundation lives under [infra/terraform](C:\Users\limke_msg9rxa\Downloads\Courses\big-data\IS459_Crypto_BigData_Pipeline\infra\terraform).

This stack provisions:
- VPC networking with 2 public subnets
- EC2 for the Docker-based streaming stack
- S3 Bronze/Silver/checkpoint storage
- Kinesis Data Streams + Firehose archival
- Glue Schema Registry resources
- IAM roles for EC2, Firehose, and EMR Serverless
- An EMR Serverless Spark application

### Required inputs

Copy the example file and replace the placeholder values:

```powershell
Copy-Item infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
```

Set at minimum:
- `project_name`
- `environment`
- `admin_cidrs`
- `ssh_public_key`

`aws_region` defaults to `us-east-1`, matching the current repo configuration.

### Usage

Authenticate with AWS before running Terraform, for example with your shell environment, AWS profile, or SSO session. Terraform in this repo does not read credentials from checked-in files.

```powershell
cd infra/terraform
terraform init
terraform fmt
terraform validate
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### Post-apply

Use the EC2 public DNS or IP from Terraform outputs to connect:

```powershell
ssh -i path\to\your-key.pem ec2-user@<ec2-public-dns>
```

The EC2 bootstrap now installs Docker, creates swap, attaches a persistent Docker data volume, and can optionally clone the repo if you set `ec2_bootstrap_repo_url` in [terraform.tfvars.example](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/infra/terraform/terraform.tfvars.example).

If you do not set `ec2_bootstrap_repo_url`, then clone or upload this repo onto the instance and start the local stack manually:

```powershell
docker compose up -d
```

Terraform now bootstraps the EC2 host. It still does not inject your runtime secrets, and it will only clone the repo automatically if you provide `ec2_bootstrap_repo_url`.

### Foundation protection

The persistent foundation is protected from accidental `terraform destroy`:
- S3 data lake bucket
- S3 bucket security/encryption configuration
- S3 placeholder prefixes
- Glue registry
- Glue schema

Those resources use Terraform `prevent_destroy`, so a full destroy will stop with an error instead of deleting your data-contract layer.

### Runtime-only teardown

If you want to tear down the disposable runtime while keeping the foundation, destroy only the runtime resources, for example:

```powershell
cd infra/terraform
terraform destroy `
  -target=aws_instance.app `
  -target=aws_kinesis_firehose_delivery_stream.bronze_archive `
  -target=aws_emrserverless_application.spark `
  -target=aws_iam_role_policy.ec2_access `
  -target=aws_iam_role_policy.firehose_access `
  -target=aws_iam_role_policy.emr_serverless_access `
  -target=aws_cloudwatch_log_stream.firehose `
  -target=aws_cloudwatch_log_group.firehose `
  -target=aws_security_group.ec2 `
  -target=aws_security_group.emr_serverless `
  -target=aws_iam_instance_profile.ec2 `
  -target=aws_iam_role_policy_attachment.ec2_ssm_core `
  -target=aws_iam_role.ec2 `
  -target=aws_iam_role.firehose `
  -target=aws_iam_role.emr_serverless_execution `
  -target=aws_key_pair.ec2 `
  -target=aws_route_table_association.public[0] `
  -target=aws_route_table_association.public[1] `
  -target=aws_route.public_internet `
  -target=aws_route_table.public `
  -target=aws_internet_gateway.main `
  -target=aws_subnet.public[0] `
  -target=aws_subnet.public[1] `
  -target=aws_vpc.main
```

Keep `aws_kinesis_stream.ohlcv` if you want a stable stream endpoint between runs. Add `-target=aws_kinesis_stream.ohlcv` only when you intentionally want to remove and later recreate the stream.

If you ever truly want to destroy the foundation too, remove the `prevent_destroy` lifecycle blocks first, run `terraform apply`, then run the destroy command.

## Airflow Local Dev

Airflow DAGs for Phase 7 live under [airflow/dags](/c:/Users/limke_msg9rxa/Downloads/Courses/big-data/IS459_Crypto_BigData_Pipeline/airflow/dags).

The local Docker stack now includes:
- Postgres for Airflow metadata
- Airflow webserver on `8088`
- Airflow scheduler
- A one-time `airflow-init` service that runs database migrations and creates the default admin user

Start Airflow locally:

```powershell
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler
docker-compose ps
```

Open `http://localhost:8088` and sign in with `admin` / `admin`.

Before the EMR-backed DAGs will run successfully, set these environment variables or Airflow Variables:
- `BATCH_S3_BUCKET`
- `AIRFLOW_EMR_SERVERLESS_APPLICATION_ID`
- `AIRFLOW_EMR_SERVERLESS_EXECUTION_ROLE_ARN`
- `AIRFLOW_BRONZE_TO_SILVER_ENTRYPOINT`
- `AIRFLOW_SILVER_TO_GOLD_ENTRYPOINT`
- `AIRFLOW_EMR_PY_FILES` if your Spark job imports shared repo modules
- `AIRFLOW_CLICKHOUSE_URL` if EMR should write aggregates and metrics into ClickHouse

The DAGs are scaffolded to submit EMR Serverless jobs; they do not run the Spark batch jobs locally inside the Airflow containers.
