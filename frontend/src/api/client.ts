import { apiBaseUrl } from "../config/env";
import type { ApiErrorResponse } from "../types/api";

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function buildUrl(path: string, query?: Record<string, string | number | undefined>) {
  const url = new URL(`${apiBaseUrl}${path}`, window.location.origin);

  for (const [key, value] of Object.entries(query ?? {})) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }

  return url.toString();
}

export async function getJson<T>(
  path: string,
  query?: Record<string, string | number | undefined>,
): Promise<T> {
  const response = await fetch(buildUrl(path, query));
  const payload = (await response.json().catch(() => null)) as ApiErrorResponse | T | null;

  if (!response.ok) {
    const message =
      payload && typeof payload === "object" && "detail" in payload && typeof payload.detail === "string"
        ? payload.detail
        : `Request failed with status ${response.status}.`;

    throw new ApiError(message, response.status);
  }

  return payload as T;
}
