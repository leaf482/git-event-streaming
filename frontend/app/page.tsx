"use client";

import { useEffect, useState } from "react";
import { StatusBadge } from "../components/StatusBadge";

type ServiceStatus = {
  ingestor: boolean;
  consumer: boolean;
};

export default function OverviewPage() {
  const [status, setStatus] = useState<ServiceStatus>({ ingestor: false, consumer: false });

  useEffect(() => {
    let active = true;

    async function checkStatus() {
      const [ingestor, consumer] = await Promise.all([
        fetch("/backend/ingestor/ready").then((response) => response.ok).catch(() => false),
        fetch("/backend/consumer/ready").then((response) => response.ok).catch(() => false),
      ]);

      if (active) {
        setStatus({ ingestor, consumer });
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
            <span className="pill">SSE dashboard updates</span>
            <span className="pill">Single-node deployment path</span>
          </div>
        </div>
      </section>
    </>
  );
}
