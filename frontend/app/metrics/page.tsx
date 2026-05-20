"use client";

import { useEffect, useState } from "react";
import { StatCard } from "../../components/StatCard";
import { formatNumber, parsePrometheusMetrics } from "../../lib/metrics";
import type { MetricsMap } from "../../lib/types";

export default function MetricsPage() {
  const [consumerMetrics, setConsumerMetrics] = useState<MetricsMap>({});
  const [ingestorMetrics, setIngestorMetrics] = useState<MetricsMap>({});

  useEffect(() => {
    let active = true;

    async function loadMetrics() {
      const [consumerText, ingestorText] = await Promise.all([
        fetch("/backend/consumer/metrics").then((response) => response.text()).catch(() => ""),
        fetch("/backend/ingestor/metrics").then((response) => response.text()).catch(() => ""),
      ]);

      if (active) {
        setConsumerMetrics(parsePrometheusMetrics(consumerText));
        setIngestorMetrics(parsePrometheusMetrics(ingestorText));
      }
    }

    loadMetrics();
    const timer = window.setInterval(loadMetrics, 3000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  const consumed = consumerMetrics.github_pulse_consumer_events_consumed_total;
  const processed = consumerMetrics.github_pulse_consumer_events_processed_total;
  const duplicates = consumerMetrics.github_pulse_consumer_duplicate_events_skipped_total;
  const redisFailures = consumerMetrics.github_pulse_consumer_redis_failures_total;
  const published = ingestorMetrics.github_pulse_ingestor_events_published_total;
  const pollErrors = ingestorMetrics.github_pulse_ingestor_poll_errors_total;

  return (
    <>
      <header className="page-header">
        <div>
          <h1>System Metrics</h1>
          <p className="muted">Auto-refreshing metrics from ingestor and consumer Prometheus-style endpoints.</p>
        </div>
        <span className="status">refresh 3s</span>
      </header>

      <section className="grid cols-3">
        <StatCard label="Events published" value={formatNumber(published)} detail="ingestor to Redpanda" />
        <StatCard label="Events consumed" value={formatNumber(consumed)} detail="consumer group reads" />
        <StatCard label="Events processed" value={formatNumber(processed)} detail="Redis ranking updates" />
        <StatCard label="Duplicate skips" value={formatNumber(duplicates)} detail="idempotency marker hits" />
        <StatCard label="Redis failures" value={formatNumber(redisFailures)} detail="consumer write/read errors" />
        <StatCard label="Poll errors" value={formatNumber(pollErrors)} detail="GitHub API polling failures" />
      </section>

      <section className="grid cols-2" style={{ marginTop: 16 }}>
        <MetricPanel title="Consumer Metrics" metrics={consumerMetrics} />
        <MetricPanel title="Ingestor Metrics" metrics={ingestorMetrics} />
      </section>
    </>
  );
}

function MetricPanel({ title, metrics }: { title: string; metrics: MetricsMap }) {
  const entries = Object.entries(metrics);

  return (
    <section className="panel">
      <h2>{title}</h2>
      <table className="table">
        <tbody>
          {entries.map(([name, value]) => (
            <tr key={name}>
              <td>{name}</td>
              <td>{formatNumber(value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
