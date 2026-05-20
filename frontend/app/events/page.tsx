"use client";

import { useEffect, useState } from "react";
import { EventList } from "../../components/EventList";
import { StatCard } from "../../components/StatCard";
import type { RecentEvent, RecentEventsResponse } from "../../lib/types";

export default function EventsPage() {
  const [events, setEvents] = useState<RecentEvent[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    fetch("/backend/consumer/api/events/recent?limit=40")
      .then((response) => response.json())
      .then((data: RecentEventsResponse) => setEvents(data.events ?? []))
      .catch(() => setEvents([]));

    const source = new EventSource("/backend/consumer/api/events/stream?limit=40");
    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("error", () => setConnected(false));
    source.addEventListener("events", (event) => {
      const data = JSON.parse(event.data) as RecentEventsResponse;
      setEvents(data.events ?? []);
    });

    return () => source.close();
  }, []);

  const latest = events[0];
  const scoreTotal = events.reduce((sum, event) => sum + event.score, 0);

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Live Event Stream</h1>
          <p className="muted">Recent weighted GitHub events after Kafka consumption and Redis idempotency checks.</p>
        </div>
        <span className={`status ${connected ? "" : "warn"}`}>SSE {connected ? "connected" : "reconnecting"}</span>
      </header>

      <section className="grid cols-3">
        <StatCard label="Recent events" value={String(events.length)} />
        <StatCard label="Latest event type" value={latest?.event_type ?? "waiting"} />
        <StatCard label="Recent score total" value={String(scoreTotal)} />
      </section>

      <section className="panel" style={{ marginTop: 16 }}>
        <h2>Processed Events</h2>
        <EventList events={events} />
      </section>
    </>
  );
}
