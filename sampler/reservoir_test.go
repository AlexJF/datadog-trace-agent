package sampler

import (
	"testing"

	"github.com/DataDog/datadog-trace-agent/model"
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
	assert.Equal(uint64(0), reservoir.traceCount)
	assert.Nil(reservoir.slots[0])

	testTrace := generateTrace(10)
	reservoir.Add(testTrace)
	assert.Equal(uint64(1), reservoir.traceCount)
	assert.Equal(testTrace, reservoir.slots[0])

	maxTrace := generateTrace(20)
	reservoir.Add(maxTrace)
	assert.Equal(uint64(2), reservoir.traceCount)
	assert.Equal(maxTrace, reservoir.slots[0])

	maxIndex := 15
	for i := 0; i < maxIndex; i++ {
		reservoir.Add(generateTrace(i))
	}
	assert.Equal(uint64(maxIndex+2), reservoir.traceCount)
	assert.Equal(maxTrace, reservoir.slots[0])
}

func TestAddFlush(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()

	testSig := Signature(10)
	testTrace := generateTrace(6)
	s.AddToReservoir(testSig, testTrace)
	originalReservoir, _ := s.reservoirs[testSig]
	flushedReservoir := s.FlushReservoir(testSig)

	assert.Equal(originalReservoir, flushedReservoir)
	storedReservoir := s.reservoirs[testSig]
	assert.NotEqual(storedReservoir, flushedReservoir)
}

func TestAddRemoveReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()

	assert.Equal(0, len(s.reservoirs))

	testSig := Signature(10)
	s.AddToReservoir(testSig, generateTrace(5))
	assert.Equal(1, len(s.reservoirs))
	s.RemoveReservoir(testSig)
	assert.Equal(0, len(s.reservoirs))
}
