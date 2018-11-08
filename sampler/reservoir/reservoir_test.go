package reservoir

import (
	"testing"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/sampler"
	"github.com/stretchr/testify/assert"
)

func generateTrace(traceID int) *model.ProcessedTrace {
	return &model.ProcessedTrace{
		Root: &model.Span{TraceID: uint64(traceID)},
	}
}

func TestAddReservoir(t *testing.T) {
	assert := assert.New(t)
	reservoir := newReservoir()
	assert.Equal(sampler.Signature(0), reservoir.TraceCount)
	assert.Nil(reservoir.Slots[0])

	testTrace := generateTrace(10)
	reservoir.Add(testTrace)
	assert.Equal(sampler.Signature(1), reservoir.TraceCount)
	assert.Equal(testTrace, reservoir.Slots[0])

	maxTrace := generateTrace(20)
	reservoir.Add(maxTrace)
	assert.Equal(sampler.Signature(2), reservoir.TraceCount)
	assert.Equal(maxTrace, reservoir.Slots[0])

	maxIndex := 15
	for i := 0; i < maxIndex; i++ {
		reservoir.Add(generateTrace(i))
	}
	assert.Equal(sampler.Signature(maxIndex+2), reservoir.TraceCount)
	assert.Equal(maxTrace, reservoir.Slots[0])
}

func TestAddFlush(t *testing.T) {
	assert := assert.New(t)
	s := NewStratifiedReservoir()

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
	s := NewStratifiedReservoir()

	assert.Equal(0, len(s.reservoirs))

	testSig := sampler.Signature(10)
	s.Add(testSig, generateTrace(5))
	assert.Equal(1, len(s.reservoirs))
	s.Remove(testSig)
	assert.Equal(0, len(s.reservoirs))
}
