package sampler

import (
	"sync"
	"sync/atomic"

	"github.com/DataDog/datadog-trace-agent/model"
)

type Reservoir struct {
	slots      []*model.ProcessedTrace
	traceCount uint64
}

func (r *Reservoir) Add(trace *model.ProcessedTrace) {
	atomic.AddUint64(&r.traceCount, 1)
	if r.slots[0] == nil {
		r.slots[0] = trace
		return
	}
	if r.slots[0].Root.TraceID < trace.Root.TraceID {
		r.slots[0] = trace
	}
}

func newReservoir() *Reservoir {
	return &Reservoir{
		slots: make([]*model.ProcessedTrace, 1),
	}
}

type StratifiedReservoir struct {
	sync.RWMutex
	reservoirs map[uint32]*Reservoir
}

func newStratifiedReservoir() *StratifiedReservoir {
	return &StratifiedReservoir{
		reservoirs: make(map[uint32]*Reservoir, 2),
	}
}

func (s *StratifiedReservoir) AddToReservoir(sig uint32, trace *model.ProcessedTrace) {
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		reservoir = newReservoir()
		// TODO smart merge to protect against unlimited sigs
		s.Lock()
		s.reservoirs[sig] = reservoir
		s.Unlock()
	}
	reservoir.Add(trace)
}

func (s *StratifiedReservoir) FlushReservoir(sig uint32) *Reservoir {
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		// Error, need to be removed from the flusher queue
		return nil
	}

	isEmpty := atomic.LoadUint64(&reservoir.traceCount) == uint64(0)
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

func (s *StratifiedReservoir) RemoveReservoir(sig uint32) {
	s.Lock()
	delete(s.reservoirs, sig)
	s.Unlock()
}
