FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y \
    libgdal-dev libgeos-dev libproj-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    geopandas pandas rapidfuzz sqlalchemy psycopg2-binary \
    camelot-py[cv] beautifulsoup4 requests openpyxl pyarrow \
    bcrypt geoalchemy2