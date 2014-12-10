package carbonmem

import (
	"math"
	"reflect"
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
		{"foo", 100, 3, 99, 103, []float64{math.NaN(), 3, math.NaN(), math.NaN(), math.NaN()}},
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

	w := NewWhisper(100, 10, 1)

	for _, tt := range tests {
		w.Set(tt.t, tt.metric, tt.count)
		if tt.expected != nil {
			r := w.Fetch(tt.metric, tt.from, tt.until)
			if !nearlyEqual(r.Values, tt.expected) {
				t.Errorf("got %#v , want %#v\n", r.Values, tt.expected)
			}
		}
	}

	id, ok := w.l.Find("foo")
	if !ok {
		t.Errorf("foo is an unknown metric")
	}

	if w.l.Active(id) {
		t.Errorf("`foo' wasn't cleared from 'active'")
	}
}

func TestMemStoreAggregate(t *testing.T) {

	var tests = []struct {
		metric   string
		t        int32
		count    uint64
		from     int32
		until    int32
		expected []float64
	}{
		{"foo", 100, 1, 95, 115, []float64{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()}},
		{"foo", 100, 3, 95, 115, []float64{math.NaN(), 3, math.NaN(), math.NaN(), math.NaN()}},
		{"foo", 107, 4, 95, 115, []float64{math.NaN(), 3, 4, math.NaN(), math.NaN()}},
		{"foo", 98, 12, 95, 115, []float64{12, 3, 4, math.NaN(), math.NaN()}},
	}

	w := NewWhisper(100, 10, 5)

	for _, tt := range tests {
		w.Set(tt.t, tt.metric, tt.count)
		if tt.expected != nil {
			r := w.Fetch(tt.metric, tt.from, tt.until)
			if r.Step != 5 || !nearlyEqual(r.Values, tt.expected) {
				t.Errorf("got %#v  (step=%d), want %#v (step=5)\n", r.Values, r.Step, tt.expected)
			}
		}
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

	w := NewWhisper(100, 10, 1)

	w.Set(100, "carbon.relays", 1)
	w.Set(100, "carbon.zipper", 1)
	w.Set(100, "carbon.rewhatever.errors", 1)
	w.Set(100, "carbon.rewhatever.count", 1)
	w.Set(100, "carbon.notmatched", 1)

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
			nil,
		},
		{
			"carbon.re*",
			[]Glob{
				{Metric: "carbon.relays", IsLeaf: true},
				{Metric: "carbon.rewhatever", IsLeaf: false},
			},
		},
		{
			"carbon.z*",
			[]Glob{
				{Metric: "carbon.zipper", IsLeaf: true},
			},
		},
		{
			"carbon.z",
			nil,
		},
		{
			"carbon.re*.errors",
			[]Glob{
				{Metric: "carbon.rewhatever.errors", IsLeaf: true},
			},
		},
		{
			"carbon.re*eve*",
			[]Glob{
				{Metric: "carbon.rewhatever", IsLeaf: false},
			},
		},
		{
			"carbon.re*.erro*",
			[]Glob{
				{Metric: "carbon.rewhatever.errors", IsLeaf: true},
			},
		},
	}

	for _, tt := range tests {
		globs := w.Find(tt.target)
		if !reflect.DeepEqual(globs, tt.want) {
			t.Errorf("Find(%v)=%#v , want %#v\n", tt.target, globs, tt.want)
		}
	}
}

func TestTopK(t *testing.T) {

	w := NewWhisper(100, 10, 2)

	w.Set(100, "carbon.rewhatever.lots", 10)
	w.Set(100, "carbon.rewhatever.fewer", 8)
	w.Set(105, "carbon.rewhatever.foo", 5)
	w.Set(105, "carbon.rewhatever.bar", 4)
	w.Set(105, "carbon.notmatched", 1)

	var tests = []struct {
		prefix  string
		seconds int32
		globs   []Glob
	}{
		{"carbon.rewhatever.*", 4,
			[]Glob{
				{Metric: "carbon.rewhatever.foo", IsLeaf: true},
				{Metric: "carbon.rewhatever.bar", IsLeaf: true},
			},
		},
		{"carbon.rewhatever.*", 6,
			[]Glob{
				{Metric: "carbon.rewhatever.lots", IsLeaf: true},
				{Metric: "carbon.rewhatever.fewer", IsLeaf: true},
				{Metric: "carbon.rewhatever.foo", IsLeaf: true},
				{Metric: "carbon.rewhatever.bar", IsLeaf: true},
			},
		},
	}

	for _, tt := range tests {
		if g := w.TopK(tt.prefix, tt.seconds); !reflect.DeepEqual(g, tt.globs) {
			t.Errorf("Topk(%v, %v)=%v, want %v", tt.prefix, tt.seconds, g, tt.globs)
		}
	}
}
