package consumer

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	startedAtUnix                  int64
	consumedTotal                  atomic.Uint64
	processedTotal                 atomic.Uint64
	duplicateSkippedTotal          atomic.Uint64
	redisFailuresTotal             atomic.Uint64
	processingFailures             atomic.Uint64
	processingLatencyNanos         atomic.Uint64
	processingLatencyBucketLE10ms  atomic.Uint64
	processingLatencyBucketLE50ms  atomic.Uint64
	processingLatencyBucketLE100ms atomic.Uint64
	processingLatencyBucketLE500ms atomic.Uint64
	processingLatencyBucketLE1s    atomic.Uint64
	processingLatencyBucketInf     atomic.Uint64
	postgresWritesTotal            atomic.Uint64
	postgresWriteFailures          atomic.Uint64
	postgresWriteLatencyNanos      atomic.Uint64
	persistenceDroppedTotal        atomic.Uint64
	historicalQueriesTotal         atomic.Uint64
	historicalQueryFailures        atomic.Uint64
	replayOperationsTotal          atomic.Uint64
	snapshotWritesTotal            atomic.Uint64
	apiRequestsTotal               atomic.Uint64
	apiErrorsTotal                 atomic.Uint64
	sseConnectionsTotal            atomic.Uint64
	sseActiveConnections           atomic.Int64
	persistenceQueueDepth          atomic.Int64
}

type MetricsSnapshot struct {
	UptimeSeconds             int64
	ConsumedTotal             uint64
	ProcessedTotal            uint64
	DuplicateSkippedTotal     uint64
	RedisFailuresTotal        uint64
	ProcessingFailures        uint64
	ProcessingLatencyNanos    uint64
	ProcessingLatencyBuckets  LatencyBuckets
	PostgresWritesTotal       uint64
	PostgresWriteFailures     uint64
	PostgresWriteLatencyNanos uint64
	PersistenceDroppedTotal   uint64
	HistoricalQueriesTotal    uint64
	HistoricalQueryFailures   uint64
	ReplayOperationsTotal     uint64
	SnapshotWritesTotal       uint64
	APIRequestsTotal          uint64
	APIErrorsTotal            uint64
	SSEConnectionsTotal       uint64
	SSEActiveConnections      int64
	PersistenceQueueDepth     int64
}

type LatencyBuckets struct {
	LE10ms  uint64
	LE50ms  uint64
	LE100ms uint64
	LE500ms uint64
	LE1s    uint64
	Inf     uint64
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
	m.recordProcessingLatencyBucket(latency)
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

func (m *Metrics) RecordAPIRequest(statusCode int) {
	m.apiRequestsTotal.Add(1)
	if statusCode >= 500 {
		m.apiErrorsTotal.Add(1)
	}
}

func (m *Metrics) RecordSSEConnected() {
	m.sseConnectionsTotal.Add(1)
	m.sseActiveConnections.Add(1)
}

func (m *Metrics) RecordSSEDisconnected() {
	m.sseActiveConnections.Add(-1)
}

func (m *Metrics) SetPersistenceQueueDepth(depth int) {
	m.persistenceQueueDepth.Store(int64(depth))
}

func (m *Metrics) recordProcessingLatencyBucket(latency time.Duration) {
	switch {
	case latency <= 10*time.Millisecond:
		m.processingLatencyBucketLE10ms.Add(1)
	case latency <= 50*time.Millisecond:
		m.processingLatencyBucketLE50ms.Add(1)
	case latency <= 100*time.Millisecond:
		m.processingLatencyBucketLE100ms.Add(1)
	case latency <= 500*time.Millisecond:
		m.processingLatencyBucketLE500ms.Add(1)
	case latency <= time.Second:
		m.processingLatencyBucketLE1s.Add(1)
	default:
		m.processingLatencyBucketInf.Add(1)
	}
}

func (m *Metrics) Snapshot(now time.Time) MetricsSnapshot {
	return MetricsSnapshot{
		UptimeSeconds:          now.Unix() - m.startedAtUnix,
		ConsumedTotal:          m.consumedTotal.Load(),
		ProcessedTotal:         m.processedTotal.Load(),
		DuplicateSkippedTotal:  m.duplicateSkippedTotal.Load(),
		RedisFailuresTotal:     m.redisFailuresTotal.Load(),
		ProcessingFailures:     m.processingFailures.Load(),
		ProcessingLatencyNanos: m.processingLatencyNanos.Load(),
		ProcessingLatencyBuckets: LatencyBuckets{
			LE10ms:  m.processingLatencyBucketLE10ms.Load(),
			LE50ms:  m.processingLatencyBucketLE50ms.Load(),
			LE100ms: m.processingLatencyBucketLE100ms.Load(),
			LE500ms: m.processingLatencyBucketLE500ms.Load(),
			LE1s:    m.processingLatencyBucketLE1s.Load(),
			Inf:     m.processingLatencyBucketInf.Load(),
		},
		PostgresWritesTotal:       m.postgresWritesTotal.Load(),
		PostgresWriteFailures:     m.postgresWriteFailures.Load(),
		PostgresWriteLatencyNanos: m.postgresWriteLatencyNanos.Load(),
		PersistenceDroppedTotal:   m.persistenceDroppedTotal.Load(),
		HistoricalQueriesTotal:    m.historicalQueriesTotal.Load(),
		HistoricalQueryFailures:   m.historicalQueryFailures.Load(),
		ReplayOperationsTotal:     m.replayOperationsTotal.Load(),
		SnapshotWritesTotal:       m.snapshotWritesTotal.Load(),
		APIRequestsTotal:          m.apiRequestsTotal.Load(),
		APIErrorsTotal:            m.apiErrorsTotal.Load(),
		SSEConnectionsTotal:       m.sseConnectionsTotal.Load(),
		SSEActiveConnections:      m.sseActiveConnections.Load(),
		PersistenceQueueDepth:     m.persistenceQueueDepth.Load(),
	}
}
