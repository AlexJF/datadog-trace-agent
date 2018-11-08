package sampler

import (
	"sync"
	"sync/atomic"

	"github.com/DataDog/datadog-trace-agent/model"
)

type Reservoir struct {
	slots      []*model.ProcessedTrace
	traceCount uint64
	shrinked   bool
	size       uint64
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
	reservoirs map[Signature]*Reservoir
	newSig     chan Signature
	size       uint64
	limit      uint64
	shrinked   bool // not thread safe
}

func (s *StratifiedReservoir) isFull() bool {
	return atomic.LoadUint64(&s.size) >= atomic.LoadUint64(&s.limit)
}

func (s *StratifiedReservoir) Shrink() {
	s.shrinked = true
}

func newStratifiedReservoir() *StratifiedReservoir {
	return &StratifiedReservoir{
		reservoirs: make(map[Signature]*Reservoir, 2),
		newSig:     make(chan Signature, 10),
	}
}

func (s *StratifiedReservoir) AddToReservoir(sig Signature, trace *model.ProcessedTrace) {
	if s.shrinked {
		sig = Signature(0)
	}
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		s.newSig <- sig
		if sig != Signature(0) && s.isFull() {
			s.AddToReservoir(Signature(0), trace)
			return
		}
		traceSize := traceApproximateSize(trace)
		reservoir = newReservoir()
		reservoir.size = traceSize
		atomic.AddUint64(&s.size, traceSize)

		s.Lock()
		s.reservoirs[sig] = reservoir
		s.Unlock()
	}
	reservoir.Add(trace)
}

func (s *StratifiedReservoir) FlushReservoir(sig Signature) *Reservoir {
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
	newReservoir.size = atomic.LoadUint64(&reservoir.size)
	s.Lock()
	reservoir, _ = s.reservoirs[sig]
	s.reservoirs[sig] = newReservoir
	s.Unlock()
	return reservoir
}

func (s *StratifiedReservoir) RemoveReservoir(sig Signature) {
	s.Lock()
	delete(s.reservoirs, sig)
	s.Unlock()
}

func traceApproximateSize(trace *model.ProcessedTrace) uint64 {
	size := len(trace.Env)
	for _, span := range trace.Trace {
		size += 44
		size += len(span.Service) + len(span.Name) + len(span.Resource)
		for k, v := range span.Meta {
			size += len(k) + len(v)
		}
		for k := range span.Metrics {
			size += len(k) + 8
		}
		for _, subValue := range trace.Sublayers[span] {
			size += 8 + len(subValue.Tag.String()) + len(subValue.Metric)
		}
	}
	return uint64(size)
}
