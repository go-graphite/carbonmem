package carbonmem

import (
	"math"
	"testing"
)

// tests for memory-backed carbon store

func TestMemStore(t *testing.T) {

	var tests = []struct {
		metric   string
		t        int32
		count    uint64
		from     int32
		until    int32
		expected []float64
	}{
		{"foo", 100, 1, 99, 103, []float64{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()}},
		{"foo", 100, 2, 99, 103, []float64{math.NaN(), 3, math.NaN(), math.NaN(), math.NaN()}},
		{"foo", 101, 4, 99, 103, []float64{math.NaN(), 3, 4, math.NaN(), math.NaN()}},
		{"foo", 105, 4, 99, 105, []float64{math.NaN(), 3, 4, math.NaN(), math.NaN(), math.NaN(), 4}},
		{"foo", 109, 9, 105, 109, []float64{4, math.NaN(), math.NaN(), math.NaN(), 9}},
		{"foo", 104, 4, 100, 109, []float64{3, 4, math.NaN(), math.NaN(), 4, 4, math.NaN(), math.NaN(), math.NaN(), 9}},
		{"foo", 98, 12, 98, 109, []float64{3, 4, math.NaN(), math.NaN(), 4, 4, math.NaN(), math.NaN(), math.NaN(), 9}},
		{"foo", 110, 10, 0, 0, nil},
		{"foo", 111, 11, 0, 0, nil},
		{"foo", 108, 4, 107, 111, []float64{math.NaN(), 4, 9, 10, 11}},
		{"bar", 126, 11, 126, 126, []float64{11}},
	}

	w := NewWhisper(100, 10)

	for _, tt := range tests {
		w.Incr(tt.t, tt.metric, tt.count)
		if tt.expected != nil {
			r := w.Fetch(tt.metric, tt.from, tt.until)
			if !nearlyEqual(r.Values, tt.expected) {
				t.Errorf("got %#v , want %#v\n", r.Values, tt.expected)
			}
		}
	}

	if _, ok := w.known["foo"]; ok {
		t.Errorf("`foo' wasn't clearned from 'known'")
	}
}

const eps = 0.0000000001

func nearlyEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		// "same"
		if math.IsNaN(v) || math.IsNaN(b[i]) {
			if math.IsNaN(v) && math.IsNaN(b[i]) {
				continue
			}
			return false
		}
		// "close enough"
		if math.Abs(v-b[i]) > eps {
			return false
		}

	}

	return true
}

func TestGlob(t *testing.T) {

	w := NewWhisper(100, 10)

	w.Incr(100, "carbon.relays", 1)
	w.Incr(100, "carbon.zipper", 1)
	w.Incr(100, "carbon.rewhatever.errors", 1)
	w.Incr(100, "carbon.rewhatever.count", 1)
	w.Incr(100, "carbon.notmatched", 1)

	var tests = []struct {
		target string
		want   []Glob
	}{
		{
			"carbon.relays",
			[]Glob{
				{Metric: "carbon.relays", IsLeaf: true},
			},
		},
		{
			"carbon.re",
			[]Glob{
				{Metric: "carbon.relays", IsLeaf: true},
				{Metric: "carbon.rewhatever", IsLeaf: false},
			},
		},
	}

	for _, tt := range tests {
		globs := w.Find(tt.target)
		if len(globs) != len(tt.want) {
			t.Errorf("Find(%v)= length %d, want %d\n", tt.target, len(globs), len(tt.want))
			t.Logf("%#v", globs)
		}
	}
}
