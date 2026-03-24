# monitor-crime-and-talent


> A pipeline that pulls crime and school data, cleans it, and puts it on a map so you can see patterns.

---

##  Why I Built This

- I wanted to explore south african crime data in each station all over the country highlighting the most prevalent crimes reported.documented in each station and look at the school perfomace data in those areas.
  
---

##  What It Does



---

## System Overview

                    ┌────────────────────────────┐
                    │        Data Sources        │
                    │----------------------------│
                    │ • Crime Data (Police)      │
                    │ • School Performance Data  │
                    └────────────┬───────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │     Airflow (Docker)       │
                    │----------------------------│
                    │ DAG:                      │
                    │ 1. Extract (Scrapers/API) │
                    │ 2. Transform (Clean/Geo)  │
                    │ 3. Load                   │
                    └────────────┬───────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │   PostgreSQL + PostGIS   │
                    │--------------------------│
                    │ Schemas:                 │
                    │ • crime                  │
                    │ • schools                │
                    │                          │
                    │ Data Types:              │
                    │ • POINT (stations)       │
                    │ • POLYGON (areas)        │
                    │                          │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │      Backend API           │
                    │       (FastAPI)        │
                    │----------------------------│
                    │ • Query PostGIS            │
                    │ • Spatial joins            │
                    │ • Aggregations             │
                    │ • Filtering logic          │
                    └────────────┬───────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │        Frontend UI         │
                    │ (React + Leaflet/Mapbox)   │
                    │----------------------------│
                    │ • Heatmaps (crime)         │
                    │ • Choropleths (schools)    │
                    │ • Filters (date/type)      │
                    │ • Interactive popups       │
                    └────────────────────────────┘

##  Tech Used

- Python  
- Pandas  
- Airflow  
- Postgres  
- Docker

---

## ▶️ How to Run

```bash
git clone 
cd 
docker-compose up              
