from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd



@dag(
    dag_id="test_dag",
    start_date=datetime(2025, 7, 27),
    schedule_interval=None,
    catchup=False,
    tags=["selenium", "ibovespa"],
)
def ibovespa_dag():

    @task()
    def fetch_and_process_codigo() -> None:
        print("OK")
        return df_codigo

    df_codigo = fetch_and_process_codigo()


ibovespa_dag = ibovespa_dag()
