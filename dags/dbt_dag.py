from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "firepoint",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def run_dbt_command(command: str):
    full_cmd = f"cd {DBT_PROJECT_DIR} && dbt {command} --profiles-dir {DBT_PROFILES_DIR}"
    print(f"running: {full_cmd}")

    result = subprocess.run(
        full_cmd, shell=True, capture_output=True, text=True
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt command failed: {command}")

    return result.stdout


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["firepoint", "dbt", "daily"],
)
def firepoint_dbt_daily():

    @task
    def dbt_seed():
        return run_dbt_command("seed")

    @task
    def dbt_build_daily():
        return run_dbt_command("build --select tag:daily")

    dbt_seed() >> dbt_build_daily()


firepoint_dbt_daily_dag = firepoint_dbt_daily()
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["firepoint", "dbt", "hourly"],
)
def firepoint_dbt_hourly():
    @task
    def dbt_build_hourly():
        return run_dbt_command("build --select tag:hourly")

    dbt_build_hourly()


firepoint_dbt_hourly_dag = firepoint_dbt_hourly()