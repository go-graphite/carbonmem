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
		{"foo", 120, 1, 119, 123, []float64{math.NaN(), 1}},
		{"foo", 120, 3, 119, 123, []float64{math.NaN(), 3}},
		{"foo", 121, 4, 119, 123, []float64{math.NaN(), 3, 4}},
		{"foo", 125, 4, 119, 125, []float64{math.NaN(), 3, 4, math.NaN(), math.NaN(), math.NaN(), 4}},
		{"foo", 129, 9, 125, 129, []float64{4, math.NaN(), math.NaN(), math.NaN(), 9}},
		{"foo", 124, 4, 120, 129, []float64{3, 4, math.NaN(), math.NaN(), 4, 4, math.NaN(), math.NaN(), math.NaN(), 9}},
		{"foo", 118, 12, 118, 129, []float64{24, math.NaN()}},
		{"foo", 130, 10, 0, 0, nil},
		{"foo", 131, 11, 0, 0, nil},
		{"foo", 128, 4, 127, 131, []float64{math.NaN(), 4, 9, 10, 11}},
		{"bar", 182, 11, 120, 240, []float64{math.NaN(), 11}},
		{"bar", 245, 22, 120, 240, []float64{11, 22}},
		{"bar", 305, 33, 120, 300, []float64{22, 33}},
	}

	w := NewWhisper(100, 10, 120)

	var fooid MetricID = math.MaxUint32

	for _, tt := range tests {
		w.Set(tt.t, tt.metric, tt.count)
		if fooid == math.MaxUint32 {
			fooid, _ = w.l.Find("foo")
		}
		if tt.expected != nil {
			r := w.Fetch(tt.metric, tt.from, tt.until)
			if !nearlyEqual(r.Values, tt.expected) {
				t.Errorf("got %#v , want %#v\n", r.Values, tt.expected)
			}
		}
	}

	if _, ok := w.l.Find("foo"); ok {
		t.Errorf("foo not pruned from known keys")
	}

	w.Set(305, "baz", 1)
	bazid, ok := w.l.Find("baz")
	if !ok || bazid != fooid {
		t.Errorf("`baz` did not reuse fooid: got (%v,%v), want %v", bazid, ok, fooid)
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

	w := NewWhisper(100, 10, 60)

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

	w := NewWhisper(100, 60, 120)

	w.Set(100, "carbon.rewhatever.lots", 10)
	w.Set(100, "carbon.rewhatever.fewer", 8)
	w.Set(161, "carbon.rewhatever.foo", 5)
	w.Set(161, "carbon.rewhatever.bar", 4)
	w.Set(161, "carbon.notmatched", 1)

	var tests = []struct {
		prefix  string
		seconds int32
		globs   []Glob
	}{
		{"carbon.rewhatever.*", 60,
			[]Glob{
				{Metric: "carbon.rewhatever.foo", IsLeaf: true},
				{Metric: "carbon.rewhatever.bar", IsLeaf: true},
			},
		},
		{"carbon.rewhatever.*", 120,
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
