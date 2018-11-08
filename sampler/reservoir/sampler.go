package reservoir

import (
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
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
