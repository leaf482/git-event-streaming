"use client";

import { useEffect, useState } from "react";
import { StatusBadge } from "../components/StatusBadge";

type ServiceStatus = {
  ingestor: boolean;
  consumer: boolean;
  grafana: boolean;
  prometheus: boolean;
};

export default function OverviewPage() {
  const [status, setStatus] = useState<ServiceStatus>({ ingestor: false, consumer: false, grafana: false, prometheus: false });

  useEffect(() => {
    let active = true;

    async function checkStatus() {
      const [ingestor, consumer, grafana, prometheus] = await Promise.all([
        fetch("/backend/ingestor/ready").then((response) => response.ok).catch(() => false),
        fetch("/backend/consumer/ready").then((response) => response.ok).catch(() => false),
        fetch("/grafana/api/health").then((response) => response.ok).catch(() => false),
        fetch("/prometheus/-/ready").then((response) => response.ok).catch(() => false),
      ]);

      if (active) {
        setStatus({ ingestor, consumer, grafana, prometheus });
      }
    }

    checkStatus();
    const timer = window.setInterval(checkStatus, 5000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  return (
    <>
      <header className="page-header">
        <div>
          <h1>System Overview</h1>
          <p className="muted">Production-style event analytics pipeline running on simple Docker Compose infrastructure.</p>
        </div>
        <div className="grid">
          <StatusBadge healthy={status.ingestor} label="ingestor" />
          <StatusBadge healthy={status.consumer} label="consumer" />
          <StatusBadge healthy={status.prometheus} label="prometheus" />
          <StatusBadge healthy={status.grafana} label="grafana" />
        </div>
      </header>

      <section className="grid cols-2">
        <div className="panel">
          <h2>Architecture Flow</h2>
          <div className="flow">
            <div>GitHub Public Events API</div>
            <div>Go ingestion service normalizes events</div>
            <div>Redpanda topic: github.events.raw.v1</div>
            <div>Go consumer applies idempotent processing</div>
            <div>Redis sorted sets and recent-event list</div>
            <div>PostgreSQL historical analytics snapshots</div>
            <div>Prometheus + Grafana observability</div>
            <div>Next.js operational dashboard over HTTP + SSE</div>
          </div>
        </div>

        <div className="panel">
          <h2>Operational Priorities</h2>
          <p className="muted">
            This dashboard is intentionally focused on pipeline health, ranking movement, recent processed events,
            and service metrics. It avoids user accounts, social features, and marketing presentation.
          </p>
          <div className="grid">
            <span className="pill">Realtime event processing</span>
            <span className="pill">Redis low-latency rankings</span>
            <span className="pill">PostgreSQL durable history</span>
            <span className="pill">Prometheus metrics</span>
            <span className="pill">Grafana dashboards</span>
            <span className="pill">SSE dashboard updates</span>
            <span className="pill">Single-node deployment path</span>
          </div>
        </div>
      </section>

      <section className="grid cols-3 overview-metrics">
        <div className="panel stat">
          <span className="label">Realtime path</span>
          <span className="value">Redis</span>
          <p className="muted">Low-latency rankings, recent events, and idempotency markers keep the dashboard responsive.</p>
        </div>
        <div className="panel stat">
          <span className="label">Durable path</span>
          <span className="value">PostgreSQL</span>
          <p className="muted">Historical windows and repository snapshots support replay, backfill, and trend APIs.</p>
        </div>
        <div className="panel stat">
          <span className="label">Operations path</span>
          <span className="value">Grafana</span>
          <p className="muted">Prometheus metrics expose throughput, latency, queue depth, failures, and recovery signals.</p>
        </div>
      </section>
    </>
  );
}
