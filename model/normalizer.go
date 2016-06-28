package model

import (
	"errors"
	"fmt"
	"time"

	log "github.com/cihub/seelog"
)

const (
	// MaxServiceLen the maximum length a service can have
	MaxServiceLen = 100
	// MaxNameLen the maximum length a name can have
	MaxNameLen = 100
	// MaxResourceLen the maximum length the resource can have
	MaxResourceLen = 5000
	// MaxTypeLen the maximum length a span type can have
	MaxTypeLen = 100
	// MaxMetaKeyLen the maximum length of metadata key
	MaxMetaKeyLen = 100
	// MaxMetaValLen the maximum length of metadata value
	MaxMetaValLen = 5000
	// MaxMetricsKeyLen the maximum length of a metric name key
	MaxMetricsKeyLen = MaxMetaKeyLen
)

var (
	// Year2000NanosecTS is an arbitrary cutoff to spot weird-looking values
	Year2000NanosecTS = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
)

// Normalize makes sure a Span is properly initialized and encloses the minimum required info
func (s *Span) Normalize() error {
	// Service
	if s.Service == "" {
		return errors.New("span.normalize: empty `Service`")
	}
	if len(s.Service) > MaxServiceLen {
		return fmt.Errorf("span.normalize: `Service` too long (max %d chars): %s", MaxServiceLen, s.Service)
	}
	// service shall comply with Datadog tag normalization
	// TODO[leo]

	// Name
	if s.Name == "" {
		return errors.New("span.normalize: empty `Name`")
	}
	if len(s.Name) > MaxNameLen {
		return fmt.Errorf("span.normalize: `Name` too long (max %d chars): %s", MaxNameLen, s.Name)
	}
	// name shall comply with Datadog metric name normalization
	var ok bool
	s.Name, ok = normMetricNameParse(s.Name)
	if !ok {
		return fmt.Errorf("span.normalize: invalid `Name`: %s", s.Name)
	}

	// Resource
	if s.Resource == "" {
		return errors.New("span.normalize: empty `Resource`")
	}
	if len(s.Resource) > MaxResourceLen {
		s.Resource = s.Resource[:MaxResourceLen]
		log.Debugf("span.normalize: truncated `Resource`: %s", s.Resource)
	}

	// TraceID & SpanID should be set in the client
	// because they uniquely define the traces and associate them into traces
	if s.TraceID == 0 {
		return errors.New("span.normalize: empty `TraceID`")
	}
	if s.SpanID == 0 {
		return errors.New("span.normalize: empty `SpanID`")
	}

	// Start & Duration as nanoseconds timestamps
	// if s.Start is very little, less than year 2000 probably a unit issue so discard
	// (or it is "le bug de l'an 2000")
	if s.Start < Year2000NanosecTS {
		return fmt.Errorf("span.normalize: invalid `Start` (must be nanosecond epoch): %d", s.Start)
	}

	if s.Duration == 0 {
		return errors.New("span.normalize: spans with zeroed `Duration` are discarded, use annotations")
	}

	// Error - Nothing to do
	// Optional data, Meta & Metrics can be nil
	// Soft fail on those
	for k, v := range s.Meta {
		modified := false

		if len(k) > MaxMetaKeyLen {
			log.Debugf("span.normalize: truncating `Meta` key (max %d chars): %s", MaxMetaKeyLen, k)
			delete(s.Meta, k)
			k = fmt.Sprintf("%s...", k[:MaxMetaKeyLen])
			modified = true
		}

		if len(v) > MaxMetaValLen {
			v = fmt.Sprintf("%s...", v[:MaxMetaValLen])
			modified = true
		}

		if modified {
			s.Meta[k] = v
		}
	}

	for k, v := range s.Metrics {
		if len(k) > MaxMetricsKeyLen {
			log.Debugf("span.normalize: truncating `Metrics` key (max %d chars): %s", MaxMetricsKeyLen, k)
			delete(s.Metrics, k)
			k = fmt.Sprintf("%s...", k[:MaxMetricsKeyLen])

			s.Metrics[k] = v
		}

	}

	// ParentID set on the client side, no way of checking

	// Type
	if len(s.Type) > MaxTypeLen {
		return fmt.Errorf("span.normalize: `Type` too long (max %d chars): %s", MaxTypeLen, s.Type)
	}

	return nil
}

// This code is borrowed from dd-go metric normalization

// fast isAlpha for ascii
func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// fast isAlphaNumeric for ascii
func isAlphaNum(b byte) bool {
	return isAlpha(b) || (b >= '0' && b <= '9')
}

// normMetricNameParse normalizes metric names with a parser instead of using
// garbage-creating string replacement routines.
func normMetricNameParse(name string) (string, bool) {
	if name == "" || len(name) > MaxNameLen {
		return name, false
	}

	var i, ptr int
	res := make([]byte, 0, len(name))

	// skip non-alphabetic characters
	for ; i < len(name) && !isAlpha(name[i]); i++ {
	}

	// if there were no alphabetic characters it wasn't valid
	if i == len(name) {
		return "", false
	}

	for ; i < len(name); i++ {
		switch {
		case isAlphaNum(name[i]):
			res = append(res, name[i])
			ptr++
		case name[i] == '.':
			// we skipped all non-alpha chars up front so we have seen at least one
			switch res[ptr-1] {
			// overwrite underscores that happen before periods
			case '_':
				res[ptr-1] = '.'
			default:
				res = append(res, '.')
				ptr++
			}
		default:
			// we skipped all non-alpha chars up front so we have seen at least one
			switch res[ptr-1] {
			// no double underscores, no underscores after periods
			case '.', '_':
			default:
				res = append(res, '_')
				ptr++
			}
		}
	}

	if res[ptr-1] == '_' {
		res = res[:ptr-1]
	}

	return string(res), true
}