# Fire Point Analytics Pipeline

## project structure

```
firepoint/
├── docker-compose.yml          # airflow + minio
├── requirements.txt
├── dags/
│   ├── etl_dag.py              # mysql + json + minio → duckdb
│   └── dbt_dag.py              # dbt seed + build (hourly/daily)
├── scripts/
│   ├── 02_insert_data.py       # faker → mysql
│   ├── 03_generate_sources.py  # json + seeds + minio csv
│   └── upload_to_minio.py      # push csv to minio bucket
├── data/
│   ├── json_sources/
│   │   └── contract_negotiations.json
│   └── minio_data/
│       └── employee_attendance.csv
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── seeds/
│   │   ├── seed_product_catalog.csv
│   │   ├── seed_currency_rates.csv
│   │   └── seed_component_categories.csv
│   └── models/
│       ├── raw/
│       ├── staging/
│       └── marts/
└── duckdb/
    └── firepoint.duckdb        (created by ETL dag)
```

## setup steps

### 1. mysql (already done)
```
create database firepoint_db;
use firepoint_db;
source 01_create_tables.sql;
```
then:
```
pip install faker mysql-connector-python
python scripts/02_insert_data.py
```

### 2. generate additional sources
```
python scripts/03_generate_sources.py
```
this creates json, seeds, and minio csv — all synced with mysql data.

### 3. start docker containers
```
cd C:\Users\dpush\Documents\DataEngineering_Proj\firepoint
docker-compose up -d
```
wait ~1 min for airflow to initialize. first run takes longer (installs pip packages).

### 4. upload csv to minio
```
pip install minio
python scripts/upload_to_minio.py
```
verify at http://localhost:9001 (login: minioadmin / minioadmin)

### 5. airflow
open http://localhost:8080 (login: admin / admin)

enable and trigger DAGs in this order:
1. `firepoint_etl` — extracts all data into duckdb
2. `firepoint_dbt_daily` — runs dbt seed + daily models
3. `firepoint_dbt_hourly` — runs hourly models

## pipeline flow

```
mysql (OLTP) ──→ airflow ETL ──→ duckdb raw schema ──→ dbt raw → stg → marts
json files   ──→ airflow ETL ──→ duckdb raw schema ──↗
minio csv    ──→ airflow ETL ──→ duckdb raw schema ──↗
seed csvs    ──→ dbt seed ──→ duckdb seeds schema
```
