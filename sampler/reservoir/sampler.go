package reservoir

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
	"github.com/DataDog/datadog-trace-agent/statsd"
)

type Sampler struct {
	stratReservoir *StratifiedReservoir
	flusher        *Flusher
	minFPS         float64
	first          time.Time
}

func NewSampler(minFPS float64, maxFPS float64) *Sampler {
	flusher := NewFlusher(maxFPS-minFPS, 30*time.Second, int(math.Round(minFPS)))
	stratReservoir := NewStratifiedReservoir()

	return &Sampler{
		minFPS:         minFPS,
		stratReservoir: stratReservoir,
		flusher:        flusher,
	}
}

func (s *Sampler) Start(decisionCb func(t *model.ProcessedTrace, sampled bool)) {
	s.flusher.Start(s.stratReservoir, func(t *model.ProcessedTrace) { decisionCb(t, true) })
	s.stratReservoir.Init(s.flusher, func(t *model.ProcessedTrace) { decisionCb(t, false) })
	go s.reportStats()
}

func (s *Sampler) Sample(t *model.ProcessedTrace) {
	s.stratReservoir.Add(sig(t), t)

	newTraceTime := time.Unix(0, t.Root.Start+t.Root.Duration)
	if s.first.IsZero() {
		s.first = newTraceTime
	}
	fmt.Println("%%% " + strconv.FormatInt(newTraceTime.Sub(s.first).Nanoseconds(), 10) + " " + t.Root.Resource[3:])

	oldestTraceInSampler := s.stratReservoir.GetLatestTime()
	isReset := newTraceTime.Sub(oldestTraceInSampler) < 0
	isEnoughTimePassed := newTraceTime.Sub(oldestTraceInSampler) >= time.Duration(1./s.minFPS)*time.Second

	if isReset || isEnoughTimePassed {
		s.flusher.TicketFlush()
	}
}

func (s *Sampler) Stop() {
	s.flusher.Stop()
}

func sig(t *model.ProcessedTrace) sampler.Signature {
	return sampler.ComputeSignatureWithRootAndEnv(t.Trace, t.Root, t.Env)
}

func (s *Sampler) reportStats() {
	flushTicker := time.NewTicker(15 * time.Second)
	defer flushTicker.Stop()

	for range flushTicker.C {
		s.stratReservoir.RLock()
		signatureCard := len(s.stratReservoir.reservoirs)
		s.stratReservoir.RUnlock()
		reservoirSize := atomic.LoadUint64(&s.stratReservoir.size)
		statsd.Client.Count("datadog.trace_agent.reservoir.memory_size", int64(reservoirSize), nil, 1)
		statsd.Client.Count("datadog.trace_agent.reservoir.signature_cardinality", int64(signatureCard), nil, 1)
		if s.stratReservoir.isFull() {
			statsd.Client.Count("datadog.trace_agent.reservoir.full", int64(1), nil, 1)
		}
	}

}
