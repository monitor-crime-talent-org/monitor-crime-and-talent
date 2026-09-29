const configuredApiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";

export const apiBaseUrl = configuredApiBaseUrl.replace(/\/$/, "");
