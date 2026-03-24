from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


from bs4 import BeautifulSoup
import requests
import pendulum
import hashlib
import json
import logging
import urllib3
import os

log = logging.getLogger(__name__)

RAW_DIR    = Path("/opt/airflow/data/raw/saps")
HASH_STORE = Path("/opt/airflow/data/.saps_hashes.json")
URL = "https://www.saps.gov.za/services/crimestats.php"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_ARGS = {
    "owner": "crime-talent",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["aneledindili4@gmail.com"],
}



def download_raw_data(**ctx):
    

    response = requests.get(URL, verify=False, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', class_='table')
    if not table:
        raise ValueError("Table not found on page")

    rows = table.find_all('tr')
    if len(rows) < 2:
        raise ValueError("No data rowsfound in table")

    data_row = rows[1]
    cols = data_row.find_all('td')
    if not cols:
        raise ValueError("No columns found in row")
    
    spreadsheet_download_tag = cols[-1].find("a")

    if not spreadsheet_download_tag:
        raise ValueError("No spreadsheet download link in this column")
    
    spreadsheet_download_link = spreadsheet_download_tag.get('href')
    if not spreadsheet_download_link:
        raise ValueError("No link to download")
    
    file_url = spreadsheet_download_link if spreadsheet_download_link.startswith('http') else f"https://www.saps.gov.za/services/{spreadsheet_download_link}"
    filename = os.path.basename(spreadsheet_download_link)
    file_path = os.path.join(RAW_DIR, filename)

    os.makedirs(RAW_DIR, exist_ok=True)

    log.info(f"starting download from {file_url}")

    with requests.get(file_url, stream=True, verify=False) as r:
        r.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    log.info(f"Downloadded {filename} to {file_path}")
    return str(file_path)

with DAG(
    dag_id='saps_crime_stats',
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), 
    schedule='@monthly',
    catchup=False,
    tags=['download', 'crime_stats']
) as dag:
    download_task = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_raw_data,
        provide_context=True
    )


