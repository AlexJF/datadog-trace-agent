package model

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type sublayerValues []SublayerValue

func (values sublayerValues) Len() int {
	return len(values)
}

func (values sublayerValues) Swap(i, j int) {
	values[i], values[j] = values[j], values[i]
}

func (values sublayerValues) Less(i, j int) bool {
	if values[i].Metric < values[j].Metric {
		return true
	} else if values[i].Metric > values[j].Metric {
		return false
	} else {
		return values[i].Tag.Value < values[j].Tag.Value
	}
}

func TestComputeSublayers(t *testing.T) {
	assert := assert.New(t)

	span := func(id, parentId uint64, service, spanType string, start, duration int64) Span {
		return Span{
			TraceID:  1,
			SpanID:   id,
			ParentID: parentId,
			Service:  service,
			Type:     spanType,
			Start:    start,
			Duration: duration,
		}
	}

	sublayerValueService := func(service string, value float64) SublayerValue {
		return SublayerValue{
			Metric: "_sublayers.duration.by_service",
			Tag:    Tag{"sublayer_service", service},
			Value:  value,
		}
	}

	sublayerValueType := func(spanType string, value float64) SublayerValue {
		return SublayerValue{
			Metric: "_sublayers.duration.by_type",
			Tag:    Tag{"sublayer_type", spanType},
			Value:  value,
		}
	}

	sublayerValueCount := func(count float64) SublayerValue {
		return SublayerValue{
			Metric: "_sublayers.span_count",
			Value:  count,
		}
	}

	tests := []struct {
		name   string
		trace  Trace
		values []SublayerValue
	}{
		// Single span
		//
		// 0  10  20  30  40  50  60  70  80  90 100
		// |===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------->
		{
			"single span",
			Trace{
				span(1, 0, "web-server", "web", 0, 100),
			},
			[]SublayerValue{
				sublayerValueService("web-server", 100),
				sublayerValueType("web", 100),
				sublayerValueCount(1),
			},
		},

		// Multiple spans
		//
		// 0  10  20  30  40  50  60  70  80  90 100
		// |===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------->
		//     <-2----->   <-4----->
		//       <-3->
		{
			"multiple spans",
			Trace{
				span(1, 0, "web-server", "web", 0, 100),
				span(2, 1, "db-server", "db", 10, 20),
				span(3, 2, "pgsql", "db", 15, 10),
				span(4, 1, "web-server", "template", 40, 20),
			},
			[]SublayerValue{
				sublayerValueService("db-server", 10),
				sublayerValueService("pgsql", 10),
				sublayerValueService("web-server", 80),
				sublayerValueType("db", 20),
				sublayerValueType("template", 20),
				sublayerValueType("web", 60),
				sublayerValueCount(4),
			},
		},

		// Multiple parallel spans with no multiple service
		// active
		//
		// 0  10  20  30  40  50  60  70  80  90 100
		// |===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------->
		//     <-2----->       <-5----->
		//       <-3----->   <--6---->
		//         <-4----->               <-7->
		{
			"multiple parallel spans no multiple service active",
			Trace{
				span(1, 0, "web-server", "web", 0, 100),
				span(2, 1, "rpc1", "rpc", 10, 20),
				span(3, 1, "rpc1", "rpc", 15, 20),
				span(4, 1, "rpc1", "rpc", 20, 20),
				span(5, 1, "rpc2", "rpc", 50, 20),
				span(6, 1, "rpc2", "rpc", 45, 20),
				span(7, 1, "rpc3", "rpc", 80, 10),
			},
			[]SublayerValue{
				sublayerValueService("rpc1", 30),
				sublayerValueService("rpc2", 25),
				sublayerValueService("rpc3", 10),
				sublayerValueService("web-server", 35),
				sublayerValueType("rpc", 65),
				sublayerValueType("web", 35),
				sublayerValueCount(7),
			},
		},

		// Parallel spans parent not waiting
		//
		// 0  10  20  30  40  50  60  70  80  90 100
		// |===|===|===|===|===|===|===|===|===|===|
		// <-1----------------->
		//         <-2---------------->
		//                         <-3------------->
		{
			"parallel spans parent not waiting",
			Trace{
				span(1, 0, "web-server", "web", 0, 50),
				span(2, 1, "rpc1", "rpc", 20, 50),
				span(3, 2, "rpc2", "rpc", 60, 40),
			},
			[]SublayerValue{
				sublayerValueService("rpc1", 40),
				sublayerValueService("rpc2", 40),
				sublayerValueService("web-server", 20),
				sublayerValueType("rpc", 80),
				sublayerValueType("web", 20),
				sublayerValueCount(3),
			},
		},

		// Multiple parallel spans multiple service active parent not waiting
		//
		// 0  10  20  30  40  50  60  70  80  90 100
		// |===|===|===|===|===|===|===|===|===|===|
		// <-1----------------->
		//         <-2----------------->
		//     <-3-------------------------------->
		//                         <-4->
		{
			"multiple parallel spans multiple service active parent not waiting",
			Trace{
				span(1, 0, "web-server", "web", 0, 50),
				span(2, 1, "rpc1", "rpc", 20, 50),
				span(3, 1, "rpc2", "rpc", 10, 90),
				span(4, 1, "rpc3", "rpc", 60, 10),
			},
			[]SublayerValue{
				sublayerValueService("rpc1", 23),
				sublayerValueService("rpc2", 63),
				sublayerValueService("rpc3", 3),
				sublayerValueService("web-server", 10),
				sublayerValueType("rpc", 90),
				sublayerValueType("web", 10),
				sublayerValueCount(4),
			},
		},

		//
		// Mix of everything
		//
		// 0  10  20  30  40  50  60  70  80  90 100 110 120 130 140 150
		// |===|===|===|===|===|===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------------------->
		//     <-2----------------->       <-3--------->
		//         <-4--------->
		//       <-5------------------->
		//                         <--6-------------------->
		//                                             <-7------------->
		{
			"mix of everything",
			Trace{
				span(1, 0, "web-server", "web", 0, 130),
				span(2, 1, "pg", "db", 10, 50),
				span(3, 1, "render", "web", 80, 30),
				span(4, 2, "pg-read", "db", 20, 30),
				span(5, 1, "redis", "cache", 15, 55),
				span(6, 1, "rpc1", "rpc", 60, 60),
				span(7, 6, "alert", "rpc", 110, 40),
			},
			[]SublayerValue{
				sublayerValueService("alert", 35),
				sublayerValueService("pg", 12),
				sublayerValueService("pg-read", 15),
				sublayerValueService("redis", 27),
				sublayerValueService("render", 15),
				sublayerValueService("rpc1", 30),
				sublayerValueService("web-server", 15),
				sublayerValueType("cache", 27),
				sublayerValueType("db", 27),
				sublayerValueType("rpc", 65),
				sublayerValueType("web", 30),
				sublayerValueCount(7),
			},
		},
	}

	for _, test := range tests {
		values := ComputeSublayers(test.trace)
		sort.Sort(sublayerValues(values))

		assert.Equal(test.values, values, "test: "+test.name)
	}
}

func TestBuildTraceTimestamps(t *testing.T) {
	assert := assert.New(t)

	span := func(id, parentId uint64, service, spanType string, start, duration int64) Span {
		return Span{
			TraceID:  1,
			SpanID:   id,
			ParentID: parentId,
			Service:  service,
			Type:     spanType,
			Start:    start,
			Duration: duration,
		}
	}

	tests := []struct {
		name     string
		trace    Trace
		expected []int64
	}{
		//
		// 0  10  20  30  40  50  60  70  80  90 100 110 120 130 140 150
		// |===|===|===|===|===|===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------------------->
		//     <-2----------------->       <-3--------->
		//         <-4--------->
		//       <-5------------------->
		//                         <--6-------------------->
		//                                             <-7------------->
		{
			"mix of everything",
			Trace{
				span(1, 0, "web-server", "web", 0, 130),
				span(2, 1, "pg", "db", 10, 50),
				span(3, 1, "render", "web", 80, 30),
				span(4, 2, "pg-read", "db", 20, 30),
				span(5, 1, "redis", "cache", 15, 55),
				span(6, 1, "rpc1", "rpc", 60, 60),
				span(7, 6, "alert", "rpc", 110, 40),
			},
			[]int64{0, 10, 15, 20, 50, 60, 70, 80, 110, 120, 130, 150},
		},
	}

	for _, test := range tests {
		actual := buildTraceTimestamps(test.trace)

		assert.Equal(test.expected, actual, "test: "+test.name)
	}
}

func TestBuildTraceActiveSpansMapping(t *testing.T) {
	assert := assert.New(t)

	span := func(id, parentId uint64, service, spanType string, start, duration int64) Span {
		return Span{
			TraceID:  1,
			SpanID:   id,
			ParentID: parentId,
			Service:  service,
			Type:     spanType,
			Start:    start,
			Duration: duration,
		}
	}

	tests := []struct {
		name       string
		trace      Trace
		timestamps []int64
		expected   map[int64][]uint64
	}{
		//
		// 0  10  20  30  40  50  60  70  80  90 100 110 120 130 140 150
		// |===|===|===|===|===|===|===|===|===|===|===|===|===|===|===|
		// <-1------------------------------------------------->
		//     <-2----------------->       <-3--------->
		//         <-4--------->
		//       <-5------------------->
		//                         <--6-------------------->
		//                                             <-7------------->
		{
			"mix of everything",
			Trace{
				span(1, 0, "web-server", "web", 0, 130),
				span(2, 1, "pg", "db", 10, 50),
				span(3, 1, "render", "web", 80, 30),
				span(4, 2, "pg-read", "db", 20, 30),
				span(5, 1, "redis", "cache", 15, 55),
				span(6, 1, "rpc1", "rpc", 60, 60),
				span(7, 6, "alert", "rpc", 110, 40),
			},
			[]int64{0, 10, 15, 20, 50, 60, 70, 80, 110, 120, 130, 150},
			map[int64][]uint64{
				0:   []uint64{1},
				10:  []uint64{2},
				15:  []uint64{2, 5},
				20:  []uint64{4, 5},
				50:  []uint64{2, 5},
				60:  []uint64{5, 6},
				70:  []uint64{6},
				80:  []uint64{3, 6},
				110: []uint64{7},
				120: []uint64{1, 7},
				130: []uint64{7},
			},
		},
	}

	for _, test := range tests {
		actual := buildTraceActiveSpansMapping(test.trace, test.timestamps)

		actualSpanIds := make(map[int64][]uint64, len(actual))
		for ts, spans := range actual {
			ids := make([]uint64, 0, len(spans))
			for _, span := range spans {
				ids = append(ids, span.SpanID)
			}

			actualSpanIds[ts] = ids
		}

		assert.Equal(test.expected, actualSpanIds, "test: "+test.name)
	}
}

func TestSetSublayersOnSpan(t *testing.T) {
	assert := assert.New(t)

	values := []SublayerValue{
		SublayerValue{
			Metric: "_sublayers.duration.by_service",
			Tag:    Tag{"sublayer_service", "pgsql"},
			Value:  30.0,
		},
		SublayerValue{
			Metric: "_sublayers.duration.by_service",
			Tag:    Tag{"sublayer_service", "pgsql-read"},
			Value:  20.0,
		},
		SublayerValue{
			Metric: "_sublayers.duration.by_type",
			Tag:    Tag{"sublayer_type", "db"},
			Value:  30.0,
		},
		SublayerValue{
			Metric: "_sublayers.span_count",
			Value:  2.0,
		},
	}

	var span Span
	SetSublayersOnSpan(&span, values)

	assert.Equal(map[string]float64{
		"_sublayers.span_count":                                      2.0,
		"_sublayers.duration.by_type.sublayer_type:db":               30.0,
		"_sublayers.duration.by_service.sublayer_service:pgsql":      30.0,
		"_sublayers.duration.by_service.sublayer_service:pgsql-read": 20.0,
	}, span.Metrics)
}

func BenchmarkSublayerThru(b *testing.B) {
	// real trace
	tr := Trace{
		Span{
			TraceID: 1, SpanID: 1, ParentID: 0,
			Start: 42, Duration: 1000000000,
			Service: "mcnulty", Type: "web",
		},
		Span{
			TraceID: 1, SpanID: 2, ParentID: 1,
			Start: 100, Duration: 200000000,
			Service: "mcnulty", Type: "sql",
		},
		Span{
			TraceID: 1, SpanID: 3, ParentID: 2,
			Start: 150, Duration: 199999000,
			Service: "master-db", Type: "sql",
		},
		Span{
			TraceID: 1, SpanID: 4, ParentID: 1,
			Start: 500000000, Duration: 500000,
			Service: "redis", Type: "redis",
		},
		Span{
			TraceID: 1, SpanID: 5, ParentID: 1,
			Start: 700000000, Duration: 700000,
			Service: "mcnulty", Type: "",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ComputeSublayers(tr)
	}
}
