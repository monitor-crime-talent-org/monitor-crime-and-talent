from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


from bs4 import BeautifulSoup
import geopandas as gpd
import pandas as pd
from pathlib import Path
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
    # "email_on_failure": True,
    # "email": ["aneledindili4@gmail.com"],
}

crime_types_to_remove = [
    '17 Community reported serious Crime',
    'Contact crime (Crimes against the person)',
    'Contact-related Crime',
    'Property-related Crime',
    'Other serious Crime',
    'TRIO Crime',
    'Crime detected as a result of police action',
    'Sexual offences',
    'Drug-related crime'
]



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
    file_path = RAW_DIR / filename

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"starting download from {file_url}")

    with requests.get(file_url, stream=True, verify=False, timeout=60) as r:
        r.raise_for_status()
        content_length = r.headers.get('Content-Length')
        with open(file_path, "wb") as f:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
        if content_length and downloaded != int(content_length):
            raise ValueError(f"Download incomplete: expected {content_length}, and got {downloaded}")

    try:
        pd.ExcelFile(file_path)
    except Exception as e:
        log.error(f"Download error: {e} for file {filename}")
        os.remove(file_path)
        raise ValueError("Downloaded file is not a valid Excel file: {e}")
    
    log.info(f"Downloadded {filename} to {file_path} (size: {downloaded} bytes)")
    return str(file_path)

def clean_data(**ctx):
    excel_file = next(RAW_DIR.glob("*.xls*"))

    if not excel_file:
        raise FileNotFoundError(f"No Excel files found in {RAW_DIR}")
    

    crime_data = pd.read_excel(excel_file, sheet_name="RAW Data", header=2)
    crime_data.columns = (
    crime_data.columns
    .str.strip()
    .str.replace("\n", " ")
    .str.replace(" ", "_")
)
    crime_data["Station"] = crime_data["Station"].astype(str)
    crime_data = crime_data[crime_data["Station"].str.contains("[A-Za-z]", na=False)]
    count_col = [c for c in crime_data.columns if 'October_2025' in str(c)][0] # <-- will deal with this later
    crime_data_clean = crime_data[['Station', 'District', 'Crime_Category', count_col, 'National_contribution_placement', 'Provincial_contribution_placement', 'Count_direction']].copy()
    crime_data_clean.columns = ['Station', 'District', 'Crime_Type', 'Crime_Count', 'National_placement', 'Provincial_placement', 'Count_direction']
    crime_data_clean['Station'] = crime_data_clean['Station'].str.strip().str.upper()
    crime_data_clean = crime_data_clean[
    ~crime_data_clean['Crime_Type'].isin(crime_types_to_remove)
]
    crime_data_clean = crime_data_clean.dropna(subset=['Crime_Type'])

    output_path = RAW_DIR / "cleaned_crime_data.parquet"
    crime_data_clean.to_parquet(output_path, index=False)  

    return str(output_path)

def load_saps_shapefile(**ctx):
    # folder = Path("/opt/airflow/data/raw")
    saps_shapefile = next(RAW_DIR.glob("*.shp*"))
    station_data = gpd.read_file(saps_shapefile)

    output_path = RAW_DIR / "shapefile.parquet"
    station_data.to_parquet(output_path, index=False)

    return str(output_path)

def merge_tables(**ctx):
    ti = ctx["ti"]

    cleaned_file = ti.xcom_pull(task_ids="clean_data")
    station_data_in_parquet = ti.xcom_pull(task_ids="load_saps_shapefile")
    crime_data = pd.read_parquet(cleaned_file)

    station_data_geodf = pd.read_parquet(station_data_in_parquet)

    crime_data = crime_data.rename(columns={"Station": "Station_name"})
    station_data_geodf = station_data_geodf.rename(columns={"COMPNT_NM": "Station_name"})

    merged = crime_data.merge(station_data_geodf, on="Station_name", how="left")
    output = RAW_DIR / "merged_table.parquet"
    merged.to_parquet(output, index=False)

    return str(output)


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
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True)
    load_shape_task = PythonOperator(
        task_id='load_saps_shapefile',
        python_callable=load_saps_shapefile,
        provide_context=True)
    merge_task = PythonOperator(task_id='merge_tables', python_callable=merge_tables, provide_context=True)
    download_task >> clean_data_task >> merge_task
    load_shape_task >> merge_task
