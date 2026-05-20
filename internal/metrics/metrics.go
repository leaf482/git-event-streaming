package metrics

import (
	"sync/atomic"
	"time"
)

type Collector struct {
	startedAtUnix        int64
	lastPollAtUnix       atomic.Int64
	lastSuccessfulAtUnix atomic.Int64
	pollTotal            atomic.Uint64
	pollErrorTotal       atomic.Uint64
	eventsReceivedTotal  atomic.Uint64
	eventsPublishedTotal atomic.Uint64
	eventsFailedTotal    atomic.Uint64
	apiRequestsTotal     atomic.Uint64
	apiErrorsTotal       atomic.Uint64
}

type Snapshot struct {
	UptimeSeconds        int64
	LastPollUnix         int64
	LastSuccessfulUnix   int64
	PollTotal            uint64
	PollErrorTotal       uint64
	EventsReceivedTotal  uint64
	EventsPublishedTotal uint64
	EventsFailedTotal    uint64
	APIRequestsTotal     uint64
	APIErrorsTotal       uint64
}

func NewCollector(now time.Time) *Collector {
	return &Collector{
		startedAtUnix: now.Unix(),
	}
}

func (c *Collector) RecordPoll(now time.Time) {
	c.pollTotal.Add(1)
	c.lastPollAtUnix.Store(now.Unix())
}

func (c *Collector) RecordPollError() {
	c.pollErrorTotal.Add(1)
}

func (c *Collector) RecordBatch(received, published, failed int, now time.Time) {
	c.eventsReceivedTotal.Add(uint64(received))
	c.eventsPublishedTotal.Add(uint64(published))
	c.eventsFailedTotal.Add(uint64(failed))
	c.lastSuccessfulAtUnix.Store(now.Unix())
}

func (c *Collector) RecordAPIRequest(statusCode int) {
	c.apiRequestsTotal.Add(1)
	if statusCode >= 500 {
		c.apiErrorsTotal.Add(1)
	}
}

func (c *Collector) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		UptimeSeconds:        now.Unix() - c.startedAtUnix,
		LastPollUnix:         c.lastPollAtUnix.Load(),
		LastSuccessfulUnix:   c.lastSuccessfulAtUnix.Load(),
		PollTotal:            c.pollTotal.Load(),
		PollErrorTotal:       c.pollErrorTotal.Load(),
		EventsReceivedTotal:  c.eventsReceivedTotal.Load(),
		EventsPublishedTotal: c.eventsPublishedTotal.Load(),
		EventsFailedTotal:    c.eventsFailedTotal.Load(),
		APIRequestsTotal:     c.apiRequestsTotal.Load(),
		APIErrorsTotal:       c.apiErrorsTotal.Load(),
	}
}
