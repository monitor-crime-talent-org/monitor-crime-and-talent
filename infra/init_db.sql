CREATE USER airflow WITH PASSWORD 'airflow';
CREATE USER anele WITH PASSWORD 'anele123';

CREATE DATABASE airflow_meta OWNER airflow;
CREATE DATABASE crime_talent_db OWNER anele;


\c crime_talent_db
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- This for fuzzy text search
GRANT ALL ON SCHEMA public TO anele;

\c airflow_meta
CREATE EXTENSION IF NOT EXISTS postgis;