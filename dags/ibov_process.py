from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, date
import os
import pandas as pd
import boto3
from io import BytesIO

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from unidecode import unidecode


date_now = str(date.today())

def fetch_ibovespa_table(search_by='Código', chromedriver_path=None, headless=True) -> pd.DataFrame:
    options = Options()
    if headless:
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-gcm-driver')
    options.add_argument('--disable-gcm-notification')

    service = Service(executable_path=chromedriver_path, log_path=os.devnull) if chromedriver_path else Service(log_path=os.devnull)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get("https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")
        wait = WebDriverWait(driver, 15)
        seg = wait.until(EC.element_to_be_clickable((By.ID, 'segment')))
        Select(seg).select_by_visible_text(search_by)
        pg = wait.until(EC.element_to_be_clickable((By.ID, 'selectPage')))
        Select(pg).select_by_visible_text('120')
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find('table', class_='table')

        headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
        rows = [[td.get_text(strip=True) for td in tr.find_all('td')] for tr in table.find('tbody').find_all('tr') if tr.find_all('td')]

        if rows and len(headers) > len(rows[0]):
            headers = headers[:len(rows[0])]

        return pd.DataFrame(rows, columns=headers)
    finally:
        driver.quit()

def adj_dataframe_by_codigo(df: pd.DataFrame):
    # df = df.rename(columns={
    #     "Código": "codigo",
    #     "Ação": "acao",
    #     "Tipo": "tipo",
    #     "Qtde. Teórica": "qtd_teorica",
    #     "Part. (%)": "participacao"
    # })
    df['data_coleta'] = date_now
    df["Código"] = df["Código"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Ação"] = df["Ação"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Tipo"] = df["Tipo"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Qtde. Teórica"] = df["Qtde. Teórica"].str.replace(".", "").str.replace(",", "").astype(int)
    df["Part. (%)"] = df["Part. (%)"].str.replace(",", ".").astype(float)
    return df

def adj_dataframe_by_setor(df: pd.DataFrame):
    # df = df.rename(columns={
    #     "Setor": "setor",
    #     "Código": "codigo",
    #     "Ação": "acao",
    #     "Tipo": "tipo",
    #     "Qtde. Teórica": "qtd_teorica",
    #     "%Setor": "participacao",
    #     "Part. (%)": "participacao_acum",
    # })
    df['data_coleta'] = date_now
    df["Setor"] = df["Setor"].apply(lambda x: unidecode(x.replace(" ", "_").replace("/", "").lower()) if isinstance(x, str) else x)
    df["Código"] = df["Código"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Ação"] = df["Ação"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Tipo"] = df["Tipo"].str.replace(" ", "_").replace("/", "").str.lower()
    df["Qtde. Teórica"] = df["Qtde. Teórica"].str.replace(".", "").str.replace(",", "").astype(int)
    df["%Setor"] = df["%Setor"].str.replace(",", ".").astype(float)
    df["Part. (%)"] = df["Part. (%)"].str.replace(",", ".").astype(float)
    return df


@dag(
    dag_id="ibov_process",
    start_date=datetime(2025, 7, 27),
    schedule_interval="0 22 * * *",
    catchup=False,
    tags=["ibovespa", "producao"]
)
def ibovespa_dag():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # with TaskGroup(group_id="etl_codigo", tooltip="Processo ETL do Código") as etl_codigo:

    #     @task(task_id="scrap_codigo")
    #     def scrap_codigo_task() -> pd.DataFrame:
    #         return fetch_ibovespa_table("Código")

    #     @task(task_id="process_codigo")
    #     def process_codigo_task(df_raw: pd.DataFrame) -> pd.DataFrame:
    #         return adj_dataframe_by_codigo(df_raw)

    #     @task(task_id="upload_codigo")
    #     def upload_codigo_task(df: pd.DataFrame):
    #         bucket = Variable.get("s3_bucket", "pos-fiap-ibovespa-data")
    #         prefix = Variable.get("s3_prefix", "raw")
    #         path = f"{prefix}/codigo/data_coleta={date_now}/data.parquet"

    #         buffer = BytesIO()
    #         df.to_parquet(buffer, index=False)
    #         buffer.seek(0)

    #         s3 = boto3.client('s3')
    #         s3.upload_fileobj(buffer, bucket, path)
    #         print(f"Upload código para s3://{bucket}/{path} concluído.")

    #     raw = scrap_codigo_task()
    #     processed = process_codigo_task(raw)
    #     upload_codigo_task(processed)

    with TaskGroup(group_id="etl_setor", tooltip="Processo ETL do Setor") as etl_setor:

        @task(task_id="scrap_setor")
        def scrap_setor_task() -> pd.DataFrame:
            return fetch_ibovespa_table("Setor de Atuação")

        @task(task_id="process_setor")
        def process_setor_task(df_raw: pd.DataFrame) -> pd.DataFrame:
            return adj_dataframe_by_setor(df_raw)

        @task(task_id="upload_setor")
        def upload_setor_task(df: pd.DataFrame):
            bucket = Variable.get("s3_bucket", "pos-fiap-ibovespa-data")
            prefix = Variable.get("s3_prefix", "raw")
            path = f"{prefix}/setor/data_coleta={date_now}/data.parquet"

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3 = boto3.client('s3')
            s3.upload_fileobj(buffer, bucket, path)
            print(f"Upload setor para s3://{bucket}/{path} concluído.")

        raw = scrap_setor_task()
        processed = process_setor_task(raw)
        upload_setor_task(processed)

    start >> etl_setor >> end

ibovespa_dag = ibovespa_dag()
