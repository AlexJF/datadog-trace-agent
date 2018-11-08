package sampler

import (
	"testing"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/stretchr/testify/assert"
)

func generateTrace(traceID int) *model.ProcessedTrace {
	root := &model.Span{TraceID: uint64(traceID)}
	return &model.ProcessedTrace{
		Root:  root,
		Trace: model.Trace{root},
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

func TestShrinkedReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	s.Shrink()
	maxTrace := generateTrace(25)
	s.AddToReservoir(Signature(5), generateTrace(5))
	s.AddToReservoir(Signature(10), maxTrace)
	s.AddToReservoir(Signature(20), generateTrace(2))
	assert.Equal(1, len(s.reservoirs))
	res := s.reservoirs[Signature(0)]
	assert.Equal(res.slots[0], maxTrace)
}

func TestSizeReservoir(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	totalSignatures := 20
	for i := 1; i <= totalSignatures; i++ {
		sig := Signature(i)
		s.AddToReservoir(sig, generateTrace(5))
		assert.Equal(sig, <-s.newSig)
		assert.Equal(i*44, int(s.size))
	}
	assert.Equal(totalSignatures*44, int(s.size))
	s.RemoveReservoir(Signature(1))
	s.RemoveReservoir(Signature(2))
	assert.Equal((totalSignatures-2)*44, int(s.size))

	s.RemoveReservoir(Signature(totalSignatures + 1))
	assert.Equal((totalSignatures-2)*44, int(s.size))
}

func TestReservoirLock(t *testing.T) {
	assert := assert.New(t)
	s := newStratifiedReservoir()
	s.limit = uint64(88)
	totalSignatures := 10
	for i := 0; i < totalSignatures; i++ {
		s.AddToReservoir(Signature(i), generateTrace(5+i))
		if i < 2 {
			assert.Equal((i+1)*44, int(s.size))
		} else {
			assert.Equal(int(s.limit), int(s.size))
		}
	}
}
