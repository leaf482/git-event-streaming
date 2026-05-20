package consumer

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	startedAtUnix          int64
	consumedTotal          atomic.Uint64
	processedTotal         atomic.Uint64
	duplicateSkippedTotal  atomic.Uint64
	redisFailuresTotal     atomic.Uint64
	processingFailures     atomic.Uint64
	processingLatencyNanos atomic.Uint64
}

type MetricsSnapshot struct {
	UptimeSeconds          int64
	ConsumedTotal          uint64
	ProcessedTotal         uint64
	DuplicateSkippedTotal  uint64
	RedisFailuresTotal     uint64
	ProcessingFailures     uint64
	ProcessingLatencyNanos uint64
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

func (m *Metrics) Snapshot(now time.Time) MetricsSnapshot {
	return MetricsSnapshot{
		UptimeSeconds:          now.Unix() - m.startedAtUnix,
		ConsumedTotal:          m.consumedTotal.Load(),
		ProcessedTotal:         m.processedTotal.Load(),
		DuplicateSkippedTotal:  m.duplicateSkippedTotal.Load(),
		RedisFailuresTotal:     m.redisFailuresTotal.Load(),
		ProcessingFailures:     m.processingFailures.Load(),
		ProcessingLatencyNanos: m.processingLatencyNanos.Load(),
	}
}
