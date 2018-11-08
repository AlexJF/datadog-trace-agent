package reservoir

import (
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
	"github.com/DataDog/datadog-trace-agent/statsd"
)

type Sampler struct {
	stratReservoir *StratifiedReservoir
	flusher        *Flusher
}

func NewSampler(targetFPS float64) *Sampler {
	flusher := NewFlusher(targetFPS, 30*time.Second)
	stratReservoir := NewStratifiedReservoir()

	return &Sampler{
		stratReservoir: stratReservoir,
		flusher:        flusher,
	}
}

func (s *Sampler) Start(decisionCb func(t *model.ProcessedTrace, sampled bool)) {
	go s.reportStats()
	s.flusher.Start(s.stratReservoir, func(t *model.ProcessedTrace) { decisionCb(t, true) })
	s.stratReservoir.Init(s.flusher, func(t *model.ProcessedTrace) { decisionCb(t, false) })
}

func (s *Sampler) Sample(t *model.ProcessedTrace) {
	s.stratReservoir.Add(sig(t), t)
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
