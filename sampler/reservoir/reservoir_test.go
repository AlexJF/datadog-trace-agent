package reservoir

import (
	"testing"
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
	"github.com/stretchr/testify/assert"
)

func generateTrace(traceID int) *model.ProcessedTrace {
	root := &model.Span{TraceID: uint64(traceID)}
	return &model.ProcessedTrace{
		Root:  root,
		Trace: model.Trace{root},
	}
}

func newStratifiedReservoir() *StratifiedReservoir {
	flusher := NewFlusher(10, 30*time.Second)
	s := NewStratifiedReservoir()
	s.Init(flusher, func(t *model.ProcessedTrace) {})
	return s
}

func TestAddReservoir(t *testing.T) {
	assert := assert.New(t)
	reservoir := newReservoir()
	assert.Equal(uint64(0), reservoir.TraceCount)
	assert.Nil(reservoir.Slots[0])

	testTrace := generateTrace(10)
	reservoir.Add(testTrace)
	assert.Equal(uint64(1), reservoir.TraceCount)
	assert.Equal(testTrace, reservoir.Slots[0])

	maxTrace := generateTrace(20)
	reservoir.Add(maxTrace)
	assert.Equal(uint64(2), reservoir.TraceCount)
	assert.Equal(maxTrace, reservoir.Slots[0])

	maxIndex := 15
	for i := 0; i < maxIndex; i++ {
		reservoir.Add(generateTrace(i))
	}
	assert.Equal(uint64(maxIndex+2), reservoir.TraceCount)
	assert.Equal(maxTrace, reservoir.Slots[0])
}

func TestAddFlush(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()

	testSig := sampler.Signature(10)
	testTrace := generateTrace(6)
	s.Add(testSig, testTrace)
	originalReservoir, _ := s.reservoirs[testSig]
	flushedReservoir := s.GetAndReset(testSig)

	assert.Equal(originalReservoir, flushedReservoir)
	storedReservoir := s.reservoirs[testSig]
	assert.NotEqual(storedReservoir, flushedReservoir)
}

func TestAddRemoveReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()

	assert.Equal(0, len(s.reservoirs))

	testSig := sampler.Signature(10)
	s.Add(testSig, generateTrace(5))
	assert.Equal(1, len(s.reservoirs))
	s.Remove(testSig)
	assert.Equal(0, len(s.reservoirs))
}

func TestShrinkedReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	s.Shrink()
	maxTrace := generateTrace(25)
	s.Add(sampler.Signature(5), generateTrace(5))
	s.Add(sampler.Signature(10), maxTrace)
	s.Add(sampler.Signature(20), generateTrace(2))
	assert.Equal(1, len(s.reservoirs))
	res := s.reservoirs[sampler.Signature(0)]
	assert.Equal(res.Slots[0], maxTrace)
}

func TestSizeReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	totalSignatures := 20
	for i := 1; i <= totalSignatures; i++ {
		sig := sampler.Signature(i)
		s.Add(sig, generateTrace(5))
		assert.Equal(i*44, int(s.size))
	}
	assert.Equal(totalSignatures*44, int(s.size))
	s.Remove(sampler.Signature(1))
	s.Remove(sampler.Signature(2))
	assert.Equal((totalSignatures-2)*44, int(s.size))

	s.Remove(sampler.Signature(totalSignatures + 1))
	assert.Equal((totalSignatures-2)*44, int(s.size))
}

func TestReservoirLock(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	s.limit = uint64(88)
	totalSignatures := 10
	for i := 0; i < totalSignatures; i++ {
		s.Add(sampler.Signature(i), generateTrace(5+i))
		if i < 2 {
			assert.Equal((i+1)*44, int(s.size))
		} else {
			assert.Equal(int(s.limit), int(s.size))
		}
	}
}
