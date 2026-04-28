# monitor-crime-and-talent


> A pipeline that pulls crime and school data, cleans it, and puts it on a map so you can see patterns.

---

##  Why I Built This

- I wanted to explore south african crime data in each station all over the country highlighting the most prevalent crimes reported.documented in each station and look at the school perfomace data in those areas.
  
---

##  My Process

- Extracted raw data from excel file for quarterly crime stats and shapefile of station locations from SAPS.
- Cleaned the excel and merged its dataframe with station locations.
- 



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
git clone https://github.com/Anele-e/monitor-crime-and-talent.git
cd monitor-crime-and-talent-1
docker-compose up
```



<!-- Last deployed: 2026-04-17 13:12:37 -->


<!-- Test Deployment: 2026-04-17 13:22:37 -->


<!-- Final Auto-Deploy Test: 2026-04-17 13:38:42 -->


<!-- Deployment Trigger: 2026-04-17 14:02:32 -->


<!-- Manual Trigger: 2026-04-17 14:45:27 -->


<!-- Final Verification: 2026-04-17 14:59:20 -->
