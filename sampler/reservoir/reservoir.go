package reservoir

import (
	"sync"
	"sync/atomic"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
)

const maxMemorySize = uint64(100000000) // 100 MB

type Reservoir struct {
	Slots      []*model.ProcessedTrace
	TraceCount uint64
	shrinked   bool
	size       uint64
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
	newSig     chan sampler.Signature
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

func NewStratifiedReservoir() *StratifiedReservoir {
	return &StratifiedReservoir{
		reservoirs: make(map[sampler.Signature]*Reservoir, 2),
		limit:      maxMemorySize,
	}
}

func (s *StratifiedReservoir) Init(flusher *Flusher, onDropCb func(t *model.ProcessedTrace)) {
	s.flusher = flusher
	s.onDropCb = onDropCb
}

func (s *StratifiedReservoir) Add(sig sampler.Signature, trace *model.ProcessedTrace) {
	if s.shrinked {
		sig = sampler.Signature(0)
	}
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if !ok {
		if sig != sampler.Signature(0) && s.isFull() {
			s.Add(sampler.Signature(0), trace)
			return
		}
		s.flusher.HandleNewSignature(sig)
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
	newReservoir.size = atomic.LoadUint64(&reservoir.size)
	s.Lock()
	reservoir, _ = s.reservoirs[sig]
	s.reservoirs[sig] = newReservoir
	s.Unlock()
	return reservoir
}

func (s *StratifiedReservoir) Remove(sig sampler.Signature) {
	var size uint64
	s.RLock()
	reservoir, ok := s.reservoirs[sig]
	s.RUnlock()
	if ok {
		size += atomic.LoadUint64(&reservoir.size)
	}
	s.Lock()
	delete(s.reservoirs, sig)
	s.size -= size
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
