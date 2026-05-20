package consumer

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	startedAtUnix             int64
	consumedTotal             atomic.Uint64
	processedTotal            atomic.Uint64
	duplicateSkippedTotal     atomic.Uint64
	redisFailuresTotal        atomic.Uint64
	processingFailures        atomic.Uint64
	processingLatencyNanos    atomic.Uint64
	postgresWritesTotal       atomic.Uint64
	postgresWriteFailures     atomic.Uint64
	postgresWriteLatencyNanos atomic.Uint64
	persistenceDroppedTotal   atomic.Uint64
	historicalQueriesTotal    atomic.Uint64
	historicalQueryFailures   atomic.Uint64
	replayOperationsTotal     atomic.Uint64
	snapshotWritesTotal       atomic.Uint64
}

type MetricsSnapshot struct {
	UptimeSeconds             int64
	ConsumedTotal             uint64
	ProcessedTotal            uint64
	DuplicateSkippedTotal     uint64
	RedisFailuresTotal        uint64
	ProcessingFailures        uint64
	ProcessingLatencyNanos    uint64
	PostgresWritesTotal       uint64
	PostgresWriteFailures     uint64
	PostgresWriteLatencyNanos uint64
	PersistenceDroppedTotal   uint64
	HistoricalQueriesTotal    uint64
	HistoricalQueryFailures   uint64
	ReplayOperationsTotal     uint64
	SnapshotWritesTotal       uint64
}

func NewMetrics(now time.Time) *Metrics {
	return &Metrics{
		startedAtUnix: now.Unix(),
	}
}

func (m *Metrics) RecordConsumed() {
	m.consumedTotal.Add(1)
}

func (m *Metrics) RecordProcessed(latency time.Duration) {
	m.processedTotal.Add(1)
	m.processingLatencyNanos.Add(uint64(latency.Nanoseconds()))
}

func (m *Metrics) RecordDuplicateSkipped() {
	m.duplicateSkippedTotal.Add(1)
}

func (m *Metrics) RecordRedisFailure() {
	m.redisFailuresTotal.Add(1)
}

func (m *Metrics) RecordProcessingFailure() {
	m.processingFailures.Add(1)
}

func (m *Metrics) RecordPostgresWrite(latency time.Duration) {
	m.postgresWritesTotal.Add(1)
	m.snapshotWritesTotal.Add(1)
	m.postgresWriteLatencyNanos.Add(uint64(latency.Nanoseconds()))
}

func (m *Metrics) RecordPostgresWriteFailure() {
	m.postgresWriteFailures.Add(1)
}

func (m *Metrics) RecordPersistenceDropped() {
	m.persistenceDroppedTotal.Add(1)
}

func (m *Metrics) RecordHistoricalQuery() {
	m.historicalQueriesTotal.Add(1)
}

func (m *Metrics) RecordHistoricalQueryFailure() {
	m.historicalQueryFailures.Add(1)
}

func (m *Metrics) RecordReplayOperation() {
	m.replayOperationsTotal.Add(1)
}

func (m *Metrics) Snapshot(now time.Time) MetricsSnapshot {
	return MetricsSnapshot{
		UptimeSeconds:             now.Unix() - m.startedAtUnix,
		ConsumedTotal:             m.consumedTotal.Load(),
		ProcessedTotal:            m.processedTotal.Load(),
		DuplicateSkippedTotal:     m.duplicateSkippedTotal.Load(),
		RedisFailuresTotal:        m.redisFailuresTotal.Load(),
		ProcessingFailures:        m.processingFailures.Load(),
		ProcessingLatencyNanos:    m.processingLatencyNanos.Load(),
		PostgresWritesTotal:       m.postgresWritesTotal.Load(),
		PostgresWriteFailures:     m.postgresWriteFailures.Load(),
		PostgresWriteLatencyNanos: m.postgresWriteLatencyNanos.Load(),
		PersistenceDroppedTotal:   m.persistenceDroppedTotal.Load(),
		HistoricalQueriesTotal:    m.historicalQueriesTotal.Load(),
		HistoricalQueryFailures:   m.historicalQueryFailures.Load(),
		ReplayOperationsTotal:     m.replayOperationsTotal.Load(),
		SnapshotWritesTotal:       m.snapshotWritesTotal.Load(),
	}
}
