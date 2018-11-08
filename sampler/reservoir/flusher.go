package reservoir

import (
	"container/list"
	"math"
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
)

type FlushBucket struct {
	Signature           sampler.Signature
	LastSuccessfulFlush time.Time
}

type Flusher struct {
	targetFPS          float64
	maxNoFlushInterval time.Duration
	onFlushCb          func(t *model.ProcessedTrace)

	buckets *list.List

	stratReservoir *StratifiedReservoir

	newSigCh chan sampler.Signature
	done     chan bool
}

func NewFlusher(targetFPS float64, maxNoFlushInterval time.Duration) *Flusher {
	return &Flusher{
		targetFPS:          targetFPS,
		maxNoFlushInterval: maxNoFlushInterval,
		buckets:            list.New(),
		newSigCh:           make(chan sampler.Signature, 50),
		done:               make(chan bool),
	}
}

func (f *Flusher) Start(stratReservoir *StratifiedReservoir, onFlushCb func(t *model.ProcessedTrace)) {
	f.stratReservoir = stratReservoir
	f.onFlushCb = onFlushCb
	go func() {
		defer close(f.done)

		flushTicker := time.NewTicker(time.Duration(math.Round((1 / f.targetFPS) * float64(time.Second))))
		defer flushTicker.Stop()

		for {
			select {
			case <-f.done:
				return
			case <-flushTicker.C:
				f.flush()
			case newSig := <-f.newSigCh:
				f.handleNewSignature(newSig)
			}
		}
	}()
}

func (f *Flusher) HandleNewSignature(sig sampler.Signature) {
	f.newSigCh <- sig
}

func (f *Flusher) flush() {
	for e := f.buckets.Front(); e != nil; e = e.Next() {
		bucket := e.Value.(*FlushBucket)
		reservoir := f.stratReservoir.GetAndReset(bucket.Signature)

		if reservoir == nil {
			if time.Since(bucket.LastSuccessfulFlush) > f.maxNoFlushInterval {
				f.removeBucket(e, bucket)
			}

			continue
		}

		f.buckets.MoveToBack(e)
		for _, trace := range reservoir.Slots {
			numSlots := uint64(len(reservoir.Slots))
			numSeen := reservoir.TraceCount

			trace.Root.SetMetric("res.slots", float64(numSlots))
			trace.Root.SetMetric("res.seen", float64(numSeen))
			trace.Root.SetMetric("res.rate", float64(numSlots/numSeen))
			f.onFlushCb(trace)
		}
		bucket.LastSuccessfulFlush = time.Now()
		break
	}
}

func (f *Flusher) removeBucket(e *list.Element, bucket *FlushBucket) {
	f.stratReservoir.Remove(bucket.Signature)
	f.buckets.Remove(e)
}

func (f *Flusher) handleNewSignature(newSig sampler.Signature) {
	f.buckets.PushBack(&FlushBucket{
		Signature: newSig,
	})
}

func (f *Flusher) Stop() {
	f.done <- true
	<-f.done
}
