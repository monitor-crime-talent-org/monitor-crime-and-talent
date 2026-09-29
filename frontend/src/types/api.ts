export interface ApiErrorResponse {
  detail?: string;
}

export interface HealthResponse {
  status: string;
}

export interface CrimeGroupsResponse {
  crime_groups: string[];
}

export type GeoJsonPosition = number[];

export interface GeoJsonGeometry {
  type: "Polygon" | "MultiPolygon";
  coordinates: GeoJsonPosition[][][] | GeoJsonPosition[][][][];
}

export interface StationProperties {
  station_name: string;
  crime_count?: number;
  crime_group?: string;
  [crimeGroup: string]: string | number | undefined;
}

export interface GeoJsonFeature<TProperties = StationProperties> {
  type: "Feature";
  geometry: GeoJsonGeometry;
  properties: TProperties;
}

export interface StationFeatureCollection {
  type: "FeatureCollection";
  features: Array<GeoJsonFeature>;
}

export interface StationSearchResponse {
  stations: string[];
}

export interface TopStation {
  station_name: string;
  crime_count: number;
}

export interface TopStationsResponse {
  crime_group: string;
  stations: TopStation[];
}
