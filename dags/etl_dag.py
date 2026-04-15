
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import json
import io
import os
from minio import Minio

DUCKDB_PATH = "/opt/airflow/duckdb/firepoint.duckdb"
JSON_PATH = "/opt/airflow/data/json_sources/contract_negotiations.json"
MYSQL_CONN_ID = "mysql_firepoint"

MYSQL_TABLES = [
    "departments", "employees", "suppliers", "components",
    "purchase_orders", "contractors", "sales_orders",
    "production_lines", "products", "production_orders",
    "production_stages", "budgets", "invoices", "payments",
    "research_projects", "milestones",
]

default_args = {
    "owner": "firepoint",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["firepoint", "etl"],
)
def firepoint_etl():

    @task
    def extract_mysql():
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn_duck = duckdb.connect(DUCKDB_PATH)
        conn_duck.execute("create schema if not exists raw")

        loaded = {}
        for table in MYSQL_TABLES:
            df = mysql_hook.get_pandas_df(f"select * from {table}")
            conn_duck.execute(f"drop table if exists raw.{table}")
            conn_duck.execute(f"create table raw.{table} as select * from df")
            loaded[table] = len(df)
            print(f"  raw.{table}: {len(df)} rows")

        conn_duck.close()
        print("mysql extraction complete")
        return loaded

    @task
    def extract_json():
        with open(JSON_PATH, "r") as f:
            negotiations = json.load(f)

        headers = []
        for neg in negotiations:
            headers.append({
                "negotiation_id": neg["negotiation_id"],
                "linked_order_id": neg["linked_order_id"],
                "contractor": neg["contractor"],
                "product_code": neg["product_code"],
                "quantity": neg["quantity"],
                "initial_list_price": neg["initial_list_price"],
                "start_date": neg["start_date"],
                "last_update": neg["last_update"],
                "num_rounds": neg["num_rounds"],
                "final_outcome": neg["final_outcome"],
                "final_unit_price": neg["final_unit_price"],
                "final_total_value": neg["final_total_value"],
                "currency": neg["currency"],
                "fire_point_contact": neg["fire_point_contact"],
                "contractor_contact": neg["contractor_contact"],
            })

        rounds = []
        for neg in negotiations:
            for r in neg["rounds"]:
                rounds.append({
                    "negotiation_id": neg["negotiation_id"],
                    "round_number": r["round_number"],
                    "date": r["date"],
                    "proposed_by": r["proposed_by"],
                    "unit_price_proposed": r["unit_price_proposed"],
                    "total_proposed": r["total_proposed"],
                    "outcome": r["outcome"],
                    "notes": r["notes"],
                })

        df_headers = pd.DataFrame(headers)
        df_rounds = pd.DataFrame(rounds)

        conn_duck = duckdb.connect(DUCKDB_PATH)
        conn_duck.execute("create schema if not exists raw")

        conn_duck.execute("drop table if exists raw.negotiations")
        conn_duck.execute("create table raw.negotiations as select * from df_headers")
        print(f"  raw.negotiations: {len(df_headers)} rows")

        conn_duck.execute("drop table if exists raw.negotiation_rounds")
        conn_duck.execute("create table raw.negotiation_rounds as select * from df_rounds")
        print(f"  raw.negotiation_rounds: {len(df_rounds)} rows")

        conn_duck.close()
        print("json extraction complete")
        return {"negotiations": len(df_headers), "rounds": len(df_rounds)}

    @task
    def extract_minio():
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        minio_access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        minio_bucket = os.getenv("MINIO_BUCKET", "firepoint")

        client = Minio(minio_endpoint, access_key=minio_access,
                       secret_key=minio_secret, secure=False)

        response = client.get_object(minio_bucket, "employee_attendance.csv")
        csv_data = response.read().decode("utf-8")
        response.close()
        response.release_conn()

        df = pd.read_csv(io.StringIO(csv_data))

        conn_duck = duckdb.connect(DUCKDB_PATH)
        conn_duck.execute("create schema if not exists raw")
        conn_duck.execute("drop table if exists raw.employee_attendance")
        conn_duck.execute("create table raw.employee_attendance as select * from df")
        print(f"  raw.employee_attendance: {len(df)} rows")

        conn_duck.close()
        print("minio extraction complete")
        return {"employee_attendance": len(df)}

    extract_mysql() >> extract_json() >> extract_minio()


firepoint_etl_dag = firepoint_etl()