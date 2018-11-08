package reservoir

import (
	"sync"
	"sync/atomic"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
)

type Reservoir struct {
	Slots      []*model.ProcessedTrace
	TraceCount uint64
}

func (r *Reservoir) Add(trace *model.ProcessedTrace) (droppedTrace *model.ProcessedTrace) {
	atomic.AddUint64(&r.TraceCount, 1)
	if r.Slots[0] == nil {
		r.Slots[0] = trace
		return
	}

	if r.Slots[0].Root.TraceID < trace.Root.TraceID {
		droppedTrace = r.Slots[0]
		r.Slots[0] = trace

		return
	}

	return trace
}

func newReservoir() *Reservoir {
	return &Reservoir{
		Slots: make([]*model.ProcessedTrace, 1),
	}
}

type StratifiedReservoir struct {
	onDropCb func(t *model.ProcessedTrace)
	flusher  *Flusher

	sync.RWMutex
	reservoirs map[sampler.Signature]*Reservoir
}

func NewStratifiedReservoir() *StratifiedReservoir {
	return &StratifiedReservoir{
		reservoirs: make(map[sampler.Signature]*Reservoir, 2),
	}
}

func (s *StratifiedReservoir) Init(flusher *Flusher, onDropCb func(t *model.ProcessedTrace)) {
	s.flusher = flusher
	s.onDropCb = onDropCb
}

func (s *StratifiedReservoir) Add(sig sampler.Signature, trace *model.ProcessedTrace) {
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		reservoir = newReservoir()
		s.flusher.HandleNewSignature(sig)
		// TODO smart merge to protect against unlimited sigs
		s.Lock()
		s.reservoirs[sig] = reservoir
		s.Unlock()
	}
	droppedTrace := reservoir.Add(trace)

	if droppedTrace != nil {
		s.onDropCb(droppedTrace)
	}
}

func (s *StratifiedReservoir) GetAndReset(sig sampler.Signature) *Reservoir {
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		// Error, need to be removed from the flusher queue
		return nil
	}

	isEmpty := atomic.LoadUint64(&reservoir.TraceCount) == uint64(0)
	if isEmpty {
		return nil
	}
	newReservoir := newReservoir()
	s.Lock()
	reservoir, _ = s.reservoirs[sig]
	s.reservoirs[sig] = newReservoir
	s.Unlock()
	return reservoir
}

func (s *StratifiedReservoir) Remove(sig sampler.Signature) {
	s.Lock()
	delete(s.reservoirs, sig)
	s.Unlock()
}
