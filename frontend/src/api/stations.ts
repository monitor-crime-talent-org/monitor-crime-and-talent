import { getJson } from "./client";
import type {
  CrimeGroupsResponse,
  HealthResponse,
  StationFeatureCollection,
  StationSearchResponse,
  TopStationsResponse,
} from "../types/api";

export function getHealth() {
  return getJson<HealthResponse>("/health");
}

export function getCrimeGroups() {
  return getJson<CrimeGroupsResponse>("/crime-groups");
}

export function getStationsGeoJson(crimeGroup?: string) {
  return getJson<StationFeatureCollection>("/stations/geojson", {
    crime_group: crimeGroup,
  });
}

export function searchStations(query: string, limit = 20) {
  return getJson<StationSearchResponse>("/stations/search", { query, limit });
}

export function getTopStations(crimeGroup: string, limit = 10) {
  return getJson<TopStationsResponse>("/stations/top", {
    crime_group: crimeGroup,
    limit,
  });
}

export function getStation(stationName: string) {
  return getJson<StationFeatureCollection["features"][number]>(
    `/stations/${encodeURIComponent(stationName)}`,
  );
}
