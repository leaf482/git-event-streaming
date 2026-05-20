import type { MetricsMap } from "./types";

export function parsePrometheusMetrics(input: string): MetricsMap {
  const metrics: MetricsMap = {};

  for (const line of input.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const [name, rawValue] = trimmed.split(/\s+/);
    const value = Number(rawValue);
    if (name && Number.isFinite(value)) {
      metrics[name] = value;
    }
  }

  return metrics;
}

export function formatNumber(value: number | undefined): string {
  if (value === undefined) {
    return "0";
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(value);
}
