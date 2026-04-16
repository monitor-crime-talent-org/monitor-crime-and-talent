from datetime import datetime, timedelta
from pathlib import Path
import zipfile
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import re
from datetime import datetime

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
    "retries": 3,
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

Violent_Crime = ["Murder", "Attempted murder", "Culpable homicide", "Assault with the intent to inflict grievous bodily harm", "Common assault",
"Robbery with aggravating circumstances", "Common robbery",
"Carjacking", "Truck hijacking", "Robbery of cash in transit",
"Bank robbery", "Robbery at residential premises", "Robbery at non-residential premises", "Public violence"]
Sexual_Offences = ["Rape", "Sexual assault", "Attempted sexual offences", "Contact sexual offences", "Sexual offences detected as a result of police action"]
Theft_and_Burglary = ["Burglary at residential premises", "Burglary at non-residential premises", "Shoplifting", "Stock-theft", "Theft of motor vehicle and motorcycle", "Theft out of or from motor vehicle", "All theft not mentioned elsewhere"]
Crimes_against_Children = ["Neglect and ill-treatment of children", "Kidnapping", "Abduction"]
Property_Damage = ["Arson", "Malicious damage to property"]
Commercial_or_Financial = ["Commercial crime"]
Police_Action = ["Driving under the influence of alcohol or drugs", "Illegal possession of firearms and ammunition"]
Social_Crimes = ["Crimen injuria"]

crime_groups = {
    "Violent Crime": Violent_Crime,
    "Sexual Offences": Sexual_Offences,
    "Theft and Burglary": Theft_and_Burglary,
    "Crimes Against Children": Crimes_against_Children,
    "Property Damage": Property_Damage,
    "Commercial Crime": Commercial_or_Financial,
    "Police Action": Police_Action,
    "Social Crime": Social_Crimes
}

def map_crime_group(crime):
    for group, crimes in crime_groups.items():
        if crime in crimes:
            return group
    return "Other"



def get_latest_quarter_column(df):
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    month_pattern = '|'.join(months)

    pattern = re.compile(
        rf'(?P<start_month>{month_pattern})_(?P<start_year>\d{{4}})_to_+?(?P<end_month>{month_pattern})_(?P<end_year>\d{{4}})',
        re.IGNORECASE
    )

    latest_col = None
    latest_end_date = None

    for col in df.columns:
        col_str = str(col)
        match = pattern.search(col_str)
        if match:
            end_month = match.group('end_month').capitalize()
            end_year = int(match.group('end_year'))

            try:
                end_date = datetime.strptime(f"{end_month} {end_year}", "%B %Y")
            except ValueError:
                continue

            if latest_end_date is None or end_date > latest_end_date:
                latest_end_date = end_date
                latest_col = col
                
    if latest_col is None:
        raise ValueError("No quarterly column found matching pattern 'Month_YYYY_to_Month_YYYY'")

    return latest_col



def download_raw_data(**ctx):

    headers = {
        "User-Agent": "Mozilla/5.0"
        }
    
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
            f.flush()
            os.fsync(f.fileno())
        if content_length and downloaded != int(content_length):
            raise ValueError(f"Download incomplete: expected {content_length}, and got {downloaded}")

    # try:
    #     test_file = pd.read_excel(file_path, sheet_name="RAW Data", header=2, nrows=5)
    # except Exception as e:
    #     log.error(f"Download error: {e} for file {filename}")
    #     os.remove(file_path)
    #     raise ValueError("Downloaded file is not a valid Excel file: {e}")
    validate_excel(file_path)
    # clean_file_path = clean_excel(file_path)
    
    log.info(f"Downloadded {filename} and sanitized file created: {file_path}")
    return str(file_path)

def validate_excel(file_path):
    try:
        with zipfile.ZipFile(file_path) as zf:
            corrupt = zf.testzip()
            if corrupt:
                raise ValueError(f"Excel file is corrupted: {corrupt}")
    except Exception as e:
        raise ValueError(f"Excel file validation failed: {e}") 
    
    try: 
        pd.read_excel(file_path, sheet_name="RAW Data")
        return True
    except Exception as e:
        raise ValueError(f"Invalid Excel file: {e}")

# def clean_excel(input_path):
#     try:
#         df = pd.read_excel(input_path, sheet_name="RAW Data", header=2)

#         clean_path = str(input_path).replace(".xlsx", "_clean.xlsx")

#         with pd.ExcelWriter(clean_path, engine="openpyxl") as writer:
#             df.to_excel(writer, sheet_name="RAW Data", index=False, header=True)

#         return clean_path

    # except Exception as e:
    #     raise ValueError(f"Raw Data Excel sheet sanitization failed: {e}")

def clean_data(**ctx):
    output_path = RAW_DIR / "cleaned_crime_data.parquet"
    if output_path.exists():
        log.info(f"Cleaned data already exists at {output_path}, skipping reprocessing.")
        return str(output_path)
    ti = ctx["ti"]
   
    excel_file = ti.xcom_pull(task_ids="download_raw_data")
    if not excel_file:
        raise ValueError("No file path received from download_raw_data")
    # excel_file = next(RAW_DIR.glob("*.xls*"))

    # if not excel_file:
    #     raise FileNotFoundError(f"No Excel files found in {RAW_DIR}")

    excel_path = Path(excel_file)
    if not excel_path.exists():
        raise FileNotFoundError(f"Downloaded file not found: {excel_path}")    
    try:
        crime_data = pd.read_excel(excel_path, sheet_name="RAW Data", header=2)
    except (zipfile.BadZipFile, Exception) as e:
        excel_path.unlink(missing_ok=True)
        raise RuntimeError(f"Corrupted Excel file {excel_path} deleted. "
                           f"Re-run the DAG to download again.") from e
    crime_data.columns = (
    crime_data.columns
    .str.strip()
    .str.replace("\n", " ")
    .str.replace(" ", "_")
)
    crime_data["Station"] = crime_data["Station"].astype(str)
    crime_data = crime_data[crime_data["Station"].str.contains("[A-Za-z]", na=False)]
    
    latest_quarter_col = get_latest_quarter_column(crime_data)
    count_col = latest_quarter_col
    # count_col = [c for c in crime_data.columns if 'October_2025' in str(c)][0]
    
    crime_data_clean = crime_data[['Station', 'District', 'Crime_Category', count_col, 'National_contribution_placement', 'Provincial_contribution_placement', 'Count_direction']].copy()
    crime_data_clean.columns = ['Station', 'District', 'Crime_Type', 'Crime_Count', 'National_placement', 'Provincial_placement', 'Count_direction']
    crime_data_clean['Station'] = crime_data_clean['Station'].str.strip().str.upper()
    crime_data_clean = crime_data_clean[
    ~crime_data_clean['Crime_Type'].isin(crime_types_to_remove)
]
    crime_data_clean = crime_data_clean.dropna(subset=['Crime_Type'])

    if "Station" in crime_data_clean.columns:
        crime_data_clean = crime_data_clean.rename(columns={"Station": "Station_name"})
    else:
        raise KeyError(f"'Station' column not found. Available columns: {crime_data_clean.columns}")
    crime_data_clean['Crime_Group'] = crime_data_clean['Crime_Type'].apply(map_crime_group)

    grouped = crime_data_clean.groupby(
    ["Station_name", "Crime_Group"]
    )["Crime_Count"].sum().reset_index()
    grouped.head()
    pivot = grouped.pivot(index='Station_name', columns='Crime_Group', values='Crime_Count').fillna(0)

    
    pivot.to_parquet(output_path, index=True)  

    return str(output_path)


def load_saps_shapefile(**ctx):
    output_path = RAW_DIR / "shapefile.parquet"

    if output_path.exists():
        log.info(f"Shapefile already processed and saved at {output_path}, skipping reprocessing.")
        return str(output_path)
    
    saps_shapefile = next(RAW_DIR.glob("*.shp") , None)
    if not saps_shapefile:
        raise FileNotFoundError("No shapefile found in raw directory")
    
    station_data = gpd.read_file(saps_shapefile)
    station_data = station_data.rename(columns={"COMPNT_NM": "Station_name"})
    station_data = station_data.drop_duplicates(subset=["Station_name"])
    geom = station_data[['Station_name', 'geometry']].set_index('Station_name')


    geom.to_parquet(output_path, index=True)

    return str(output_path)

def merge_tables(**ctx):
    output = RAW_DIR / "merged_table.parquet"
    if output.exists():
        log.info(f"Merged table already exists at {output}, skipping reprocessing.")
        return str(output)
    
    ti = ctx["ti"]

    cleaned_file = ti.xcom_pull(task_ids="clean_data")
    station_data = ti.xcom_pull(task_ids="load_saps_shapefile")

    crime_data = pd.read_parquet(cleaned_file)
    station_data_geodf = gpd.read_parquet(station_data)

    # crime_data = crime_data.set_index("Station_name")
    # station_data_geodf = station_data_geodf.set_index("Station_name")


    gdf = crime_data.join(station_data_geodf).reset_index()
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
    gdf.crs = 'EPSG:4326'
    # sample_gdf = gdf.sample(100, random_state=42)
    output = RAW_DIR / "merged_table.parquet"
    gdf.to_parquet(output, index=False)

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
    load_shapefile_task = PythonOperator(
        task_id='load_saps_shapefile',
        python_callable=load_saps_shapefile,
        provide_context=True)

    merge_task = PythonOperator(
        task_id='merge_tables',
        python_callable=merge_tables,
        provide_context=True)
    download_task >> clean_data_task >> merge_task
    load_shapefile_task >> merge_task
    
