package reservoir

import (
	"container/list"
	"fmt"
	"math"
	"strconv"
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

	tickets     chan struct{}
	ticketFlush chan func(r time.Time)

	buckets *list.List

	stratReservoir *StratifiedReservoir

	newSigCh chan sampler.Signature
	done     chan bool
}

func NewFlusher(targetFPS float64, maxNoFlushInterval time.Duration, numTickets int) *Flusher {
	tickets := make(chan struct{}, numTickets)

	for i := 0; i < numTickets; i++ {
		tickets <- struct{}{}
	}

	return &Flusher{
		targetFPS:          targetFPS,
		maxNoFlushInterval: maxNoFlushInterval,
		buckets:            list.New(),
		newSigCh:           make(chan sampler.Signature, 50),
		done:               make(chan bool),
		tickets:            tickets,
		ticketFlush:        make(chan func(r time.Time)),
	}
}

func (f *Flusher) Start(stratReservoir *StratifiedReservoir, onFlushCb func(t *model.ProcessedTrace)) {
	f.stratReservoir = stratReservoir
	f.onFlushCb = onFlushCb
	go func() {
		defer close(f.done)

		flushTicker := time.NewTicker(time.Duration(math.Round((1 / f.targetFPS) * float64(time.Second))))
		defer flushTicker.Stop()

		ticketTicker := time.NewTicker(time.Duration(math.Round(1 / float64(cap(f.tickets)) * float64(time.Second))))
		defer ticketTicker.Stop()

		start := time.Now()
		ticked := 0
		flushed := 0
		defer func() {
			duration := time.Since(start)

			fmt.Printf("Observed ticks per second: %f\n", float64(ticked)/duration.Seconds())
			fmt.Printf("Observed flushes per second: %f\n", float64(flushed)/duration.Seconds())
		}()

		for {
			select {
			case <-f.done:
				return
			case <-flushTicker.C:
				ticked++
				flushedTrace := f.flush()
				if flushedTrace != nil {
					flushed++
				}
			case <-ticketTicker.C:
				select {
				case f.tickets <- struct{}{}:
				default:
				}
			case cb := <-f.ticketFlush:
				flushedTrace := f.flush()
				var flushedTraceTime time.Time
				if flushedTrace != nil {
					flushedTraceTime = time.Unix(0, flushedTrace.Root.Start+flushedTrace.Root.Duration)
				}
				cb(flushedTraceTime)
				if flushedTrace == nil {
					select {
					case f.tickets <- struct{}{}:
					default:
					}
				}
			case newSig := <-f.newSigCh:
				f.handleNewSignature(newSig)
			}
		}
	}()
}

func (f *Flusher) HandleNewSignature(sig sampler.Signature) {
	f.newSigCh <- sig
}

func (f *Flusher) TicketFlush() time.Time {
	select {
	case <-f.tickets:
		flushDone := make(chan bool)
		var result time.Time
		f.ticketFlush <- func(r time.Time) {
			result = r
			close(flushDone)
		}
		<-flushDone
		return result
	default:
		return time.Time{}
	}
}

func (f *Flusher) flush() *model.ProcessedTrace {
	f.stratReservoir.PrintReservoirs()
	for e := f.buckets.Front(); e != nil; e = e.Next() {
		bucket := e.Value.(*FlushBucket)
		reservoir := f.stratReservoir.GetAndReset(bucket.Signature)

		if reservoir == nil {
			if time.Since(bucket.LastSuccessfulFlush) > f.maxNoFlushInterval {
				f.removeBucket(e, bucket)
			}

			continue
		}

		if len(reservoir.Slots) == 0 {
			println("$$$$$$$$$$$$$")
		}

		f.buckets.MoveToBack(e)
		bucket.LastSuccessfulFlush = time.Now()
		for _, trace := range reservoir.Slots {
			numSlots := uint64(len(reservoir.Slots))
			numSeen := reservoir.TraceCount

			trace.Root.Meta["res.limit"] = strconv.FormatBool(bucket.Signature == 0)
			trace.Root.SetMetric("res.slots", float64(numSlots))
			trace.Root.SetMetric("res.seen", float64(numSeen))
			trace.Root.SetMetric("res.rate", float64(numSlots/numSeen))
			f.onFlushCb(trace)
			return trace
		}
	}

	return nil
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
