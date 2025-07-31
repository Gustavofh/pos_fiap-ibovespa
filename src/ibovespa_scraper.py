import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time


def fetch_ibovespa_table(
    search_by: str = 'Código',
    chromedriver_path: str = None,
    headless: bool = True
) -> pd.DataFrame:
    """
    Abre a página em modo headless, seleciona 'Consulta por' dinamicamente e
    'Resultados por página' = 120, espera a atualização e retorna um DataFrame
    preservando exatamente os valores de texto da tabela (incluindo vírgulas).
    Suprime logs do ChromeDriver e do Chrome.
    """
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
        driver.get(
            "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        )
        wait = WebDriverWait(driver, 15)

        seg = wait.until(EC.element_to_be_clickable((By.ID, 'segment')))
        Select(seg).select_by_visible_text(search_by)

        pg = wait.until(EC.element_to_be_clickable((By.ID, 'selectPage')))
        Select(pg).select_by_visible_text('120')

        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find('table', class_='table')

        headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]

        rows = []
        for tr in table.find('tbody').find_all('tr'):
            cols = [td.get_text(strip=True) for td in tr.find_all('td')]
            if cols:
                rows.append(cols)

        if rows:
            ncols = len(rows[0])
            if len(headers) > ncols:
                headers = headers[:ncols]

        df = pd.DataFrame(rows, columns=headers)
        return df

    finally:
        driver.quit()
