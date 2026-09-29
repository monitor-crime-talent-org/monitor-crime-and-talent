CREATE USER airflow WITH PASSWORD 'airflow';

CREATE DATABASE airflow_meta OWNER airflow;


\c crime_talent_db
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- This for fuzzy text search
GRANT ALL ON SCHEMA public TO crime_talent;

\c airflow_meta
CREATE EXTENSION IF NOT EXISTS postgis;
