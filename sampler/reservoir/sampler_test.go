package reservoir

import (
	"testing"
	"time"

	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/stretchr/testify/assert"
)

func TestNewSampler(t *testing.T) {
	assert := assert.New(t)
	sampler := NewSampler(1)
	pkgChan := make(chan bool, 10)

	sampler.Start(func(t *model.ProcessedTrace, sampled bool) { pkgChan <- sampled })

	sampler.Sample(generateTrace(1))
	time.Sleep(1 * time.Second)
	sampled := <-pkgChan
	assert.Equal(true, sampled)

	for j := 2; j < 4; j++ {
		sampler.Sample(generateTrace(j))
		sampler.Sample(generateTrace(j))
		sampled = <-pkgChan
		assert.Equal(false, sampled)
	}
}
