"""Read API for the crime and school-performance map.

The SAPS Airflow DAG materialises station boundaries and crime-group totals in
``public.saps_crime_stats``. This service exposes that materialised dataset in
the formats a map client needs without owning or mutating the pipeline data.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError


STATS_TABLE = "saps_crime_stats"
STATS_SCHEMA = "public"
NON_CRIME_COLUMNS = {"station_name", "geometry", "ingested_at"}
CRIME_GROUP_COLUMNS = {
    "Violent Crime": "violent_crime",
    "Sexual Offences": "sexual_offences",
    "Theft and Burglary": "theft_and_burglary",
    "Crimes Against Children": "crimes_against_children",
    "Property Damage": "property_damage",
    "Commercial Crime": "commercial_crime",
    "Police Action": "police_action",
    "Social Crime": "social_crime",
    "Other": "other",
}
MAX_PAGE_SIZE = 100

app = FastAPI(
    title="Crime and Talent API",
    version="1.0.0",
    description="Map-ready SAPS crime statistics by police-station boundary.",
)

_engine: Engine | None = None


def database_engine() -> Engine:
    """Create the database pool lazily so docs can load before Docker is ready."""
    global _engine
    if _engine is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=503, detail="DATABASE_URL is not configured."
            )
        _engine = create_engine(database_url, pool_pre_ping=True)
    return _engine


@contextmanager
def stats_connection() -> Iterator[Connection]:
    try:
        with database_engine().connect() as connection:
            if not inspect(connection).has_table(STATS_TABLE, schema=STATS_SCHEMA):
                raise HTTPException(
                    status_code=503,
                    detail="Crime data is not available yet. Run the saps_crime_stats DAG.",
                )
            yield connection
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=503, detail="The crime database is currently unavailable."
        ) from exc


def crime_columns(connection: Connection) -> dict[str, str]:
    """Return public crime-group labels mapped to known SQL column names."""
    columns = inspect(connection).get_columns(STATS_TABLE, schema=STATS_SCHEMA)
    names = {column["name"] for column in columns}
    return {
        label: column
        for label, column in CRIME_GROUP_COLUMNS.items()
        if column in names - NON_CRIME_COLUMNS
    }


def resolve_crime_group(connection: Connection, crime_group: str | None) -> str | None:
    if crime_group is None:
        return None
    groups = crime_columns(connection)
    normalized = crime_group.strip().casefold()
    for label, column in groups.items():
        if label.casefold() == normalized or column.casefold() == normalized.replace(" ", "_"):
            return column
    raise HTTPException(
        status_code=422,
        detail={"message": "Unknown crime group.", "available_groups": sorted(groups)},
    )


def feature(row: Any, crime_group: str | None = None) -> dict[str, Any]:
    properties = {
        key: value
        for key, value in dict(row._mapping).items()
        if key not in {"geometry", "station_name"} and value is not None
    }
    properties["station_name"] = row.station_name
    if crime_group:
        properties["crime_group"] = crime_group
    return {
        "type": "Feature",
        "geometry": row.geometry,
        "properties": properties,
    }


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "crime-and-talent-api", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health() -> dict[str, str]:
    with stats_connection() as connection:
        connection.execute(text("SELECT 1"))
    return {"status": "ok", "dataset": STATS_TABLE}


@app.get("/crime-groups")
def list_crime_groups() -> dict[str, list[str]]:
    with stats_connection() as connection:
        return {"crime_groups": sorted(crime_columns(connection))}


@app.get("/stations/geojson")
def stations_geojson(crime_group: str | None = None) -> dict[str, Any]:
    """Return all station boundaries as a GeoJSON FeatureCollection."""
    with stats_connection() as connection:
        group_column = resolve_crime_group(connection, crime_group)
        if group_column:
            rows = connection.execute(
                text(
                    f"SELECT station_name, ST_AsGeoJSON(geometry)::json AS geometry, "
                    f"COALESCE({group_column}, 0) AS crime_count "
                    f"FROM {STATS_SCHEMA}.{STATS_TABLE} ORDER BY station_name"
                )
            )
        else:
            rows = connection.execute(
                text(
                    f"SELECT station_name, ST_AsGeoJSON(geometry)::json AS geometry "
                    f"FROM {STATS_SCHEMA}.{STATS_TABLE} ORDER BY station_name"
                )
            )
        return {
            "type": "FeatureCollection",
            "features": [feature(row, crime_group) for row in rows],
        }


@app.get("/stations/search")
def search_stations(
    query: str = Query(min_length=2, max_length=100),
    limit: int = Query(default=20, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, list[str]]:
    with stats_connection() as connection:
        rows = connection.execute(
            text(
                f"SELECT station_name FROM {STATS_SCHEMA}.{STATS_TABLE} "
                "WHERE station_name ILIKE :query ORDER BY station_name LIMIT :limit"
            ),
            {"query": f"%{query.strip()}%", "limit": limit},
        )
        return {"stations": [row.station_name for row in rows]}


@app.get("/stations/top")
def top_stations(
    crime_group: str = Query(min_length=1),
    limit: int = Query(default=10, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    with stats_connection() as connection:
        group_column = resolve_crime_group(connection, crime_group)
        rows = connection.execute(
            text(
                f"SELECT station_name, COALESCE({group_column}, 0) AS crime_count "
                f"FROM {STATS_SCHEMA}.{STATS_TABLE} "
                f"ORDER BY {group_column} DESC NULLS LAST, station_name LIMIT :limit"
            ),
            {"limit": limit},
        )
        return {
            "crime_group": crime_group,
            "stations": [dict(row._mapping) for row in rows],
        }


@app.get("/stations/{station_name}")
def station(station_name: str) -> dict[str, Any]:
    with stats_connection() as connection:
        groups = crime_columns(connection)
        projections = ", ".join(
            f"COALESCE({column}, 0) AS {column}" for column in groups.values()
        )
        row = connection.execute(
            text(
                f"SELECT station_name, ST_AsGeoJSON(geometry)::json AS geometry, {projections} "
                f"FROM {STATS_SCHEMA}.{STATS_TABLE} "
                "WHERE LOWER(station_name) = LOWER(:station_name)"
            ),
            {"station_name": station_name.strip()},
        ).one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="Police station not found.")
        return feature(row)


@app.get("/stations/{station_name}/schools")
def station_schools(station_name: str) -> None:
    raise HTTPException(
        status_code=501,
        detail=(
            "School-performance data has not been ingested yet; this endpoint will "
            "be enabled when the schools pipeline and PostGIS table are added."
        ),
    )
