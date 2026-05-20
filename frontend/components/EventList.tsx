import type { RecentEvent } from "../lib/types";

type EventListProps = {
  events: RecentEvent[];
};

export function EventList({ events }: EventListProps) {
  return (
    <div className="event-list">
      {events.map((event) => (
        <article className="event" key={event.event_id}>
          <div>
            <span className="pill">{event.event_type}</span>
          </div>
          <strong>{event.repo_name}</strong>
          <span className="muted">
            score +{event.score} · actor {event.actor_login || "unknown"} · processed{" "}
            {new Date(event.processed_at).toLocaleTimeString()}
          </span>
        </article>
      ))}
    </div>
  );
}
