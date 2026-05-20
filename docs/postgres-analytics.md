# PostgreSQL Analytics Persistence

Phase 4 adds PostgreSQL as the durable historical analytics layer while Redis remains the realtime hot path.

## Responsibilities

- Redis stores current low-latency rankings, recent events, SSE snapshots, and short-lived idempotency markers.
- PostgreSQL stores durable event metadata, hourly repository score snapshots, aggregation window metadata, and queryable history.
- The consumer updates Redis first. Successful Redis processing then enqueues a PostgreSQL write on an async worker so a temporary database outage does not stop realtime ranking updates.

## Schema

- `processed_event_metadata`: one row per GitHub event ID. The primary key makes replay and duplicate delivery safe.
- `aggregation_windows`: catalog of persisted time windows. The current implementation uses one-hour windows.
- `repository_score_snapshots`: per-repository score and event count for each window.
- `repository_hourly_summaries`: coarse hourly summary foundation for longer retention and rollups.

The schema intentionally uses `repo_name` as the primary analytical key to avoid premature normalization.

## Replay And Backfill

Replay foundation is provided by:

```sh
make replay
```

The replay command runs `/app/replay` with a separate Kafka consumer group by default. It reads `github.events.raw.v1`, reapplies Redis aggregation, and upserts PostgreSQL metadata/snapshots. Writes are replay-safe because Redis uses event ID idempotency markers and PostgreSQL uses `ON CONFLICT` on event IDs and window keys.

Operational guidance:

- Use a separate `KAFKA_GROUP_ID` for replay so the live consumer offsets are not disturbed.
- Prefer running replay during low traffic on small EC2 instances.
- Stop the live consumer only when you intentionally want to rebuild Redis from scratch.
- Back up PostgreSQL before large replays or schema changes.

## Retention Strategy

Baseline retention:

- Keep Redpanda raw events for the configured topic retention window.
- Keep Redis realtime windows short via TTLs.
- Keep PostgreSQL event metadata and hourly snapshots as the durable analytics source.

Future cleanup can delete old `processed_event_metadata` rows after the maximum replay window while preserving hourly summaries. For example, after daily rollups exist, retain raw metadata for 30-90 days and keep summarized windows longer.

## Recovery

If PostgreSQL is unavailable, the consumer logs persistence failures and continues Redis realtime processing. Historical APIs return `503` when persistence is disabled and `500` for query errors.

Recovery flow:

1. Restore PostgreSQL connectivity.
2. Confirm `curl http://localhost:8081/metrics` shows no increasing write failures.
3. Run `make replay` if historical gaps need to be filled from retained Redpanda data.
4. Check `GET /api/history/trending/repos` and `GET /api/history/windows`.
