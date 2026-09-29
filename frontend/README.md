# Frontend Foundation

This directory contains the frontend runtime, API boundary, and domain types. It intentionally has no user interface yet.

## Local development

1. Copy `.env.example` to `.env.local` if the API location needs to change.
2. Run `npm install`.
3. Run `npm run dev`.

By default, Vite proxies `/api` to the FastAPI service at `http://localhost:8000`, so browser requests do not need a separate CORS configuration during local development.

## API modules

- `src/api/stations.ts` maps the current FastAPI station and crime endpoints.
- `src/types/api.ts` defines the GeoJSON and response types used by map and data views.
- `src/config/env.ts` centralizes the API base URL.
