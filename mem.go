// memory-backed carbon store: speaks graphite in, zipper out
package carbonmem

import (
	"math"

	"sync"
)

// Whisper is an in-memory whisper-like store
type Whisper struct {
	sync.Mutex
	t0     int64
	idx    int
	epochs []map[string]uint64
	known  map[string]int // metric -> #epochs it appears in
}

func NewWhisper(t0 int64, cap int) *Whisper {

	epochs := make([]map[string]uint64, cap)
	epochs[0] = make(map[string]uint64)

	return &Whisper{
		t0:     t0,
		epochs: epochs,
		known:  make(map[string]int),
	}
}

func (w *Whisper) Incr(t int64, metric string, val uint64) {

	// based on github.com/dgryski/go-timewindow

	if t == w.t0 {
		m := w.epochs[w.idx]

		// have we seen this metric this epoch?
		v, ok := m[metric]
		if !ok {
			// one more occurrence of this metric
			w.known[metric]++
		}

		m[metric] = v + val
		return
	}

	if t > w.t0 {
		// advance the buffer, decrementing counts for all entries in the
		// maps we pass by

		for w.t0 < t {
			w.t0++
			w.idx++
			if w.idx >= len(w.epochs) {
				w.idx = 0
			}

			m := w.epochs[w.idx]
			if m != nil {
				for k, _ := range m {
					w.known[k]--
					if w.known[k] == 0 {
						delete(w.known, k)
					}
				}
				w.epochs[w.idx] = nil
			}
		}
		w.known[metric]++
		w.epochs[w.idx] = map[string]uint64{metric: val}
		return
	}

	// less common -- update the past
	back := int(w.t0 - t)

	if back >= len(w.epochs) {
		// too far in the past, ignore
		return
	}

	idx := w.idx - back

	if idx < 0 {
		// need to wrap around
		idx += len(w.epochs)
	}

	m := w.epochs[idx]
	if m == nil {
		m = make(map[string]uint64)
		w.epochs[idx] = m
	}
	v, ok := m[metric]
	if !ok {
		w.known[metric]++
	}
	m[metric] = v + val
}

type Fetched struct {
	from   int64
	until  int64
	step   int64
	values []float64
}

func (w *Whisper) Fetch(metric string, from int64, until int64) *Fetched {

	if from > w.t0 {
		return nil
	}

	if _, ok := w.known[metric]; !ok {
		return nil
	}

	if until < from {
		return nil
	}

	if min := w.t0 - int64(len(w.epochs)) + 1; from < min {
		from = min
	}

	idx := w.idx - int(w.t0-from)
	if idx < 0 {
		idx += len(w.epochs)
	}

	points := until - from + 1 // inclusive of 'until'
	r := &Fetched{
		from:   from,
		until:  until,
		step:   1,
		values: make([]float64, points),
	}

	for p, t := 0, idx; p < int(points); p, t = p+1, t+1 {
		if t >= len(w.epochs) {
			t = 0
		}

		m := w.epochs[t]
		if v, ok := m[metric]; ok {
			r.values[p] = float64(v)
		} else {
			r.values[p] = math.NaN()
		}
	}

	return r
}

type Glob struct {
	Metric string
	IsLeaf bool
}

func (w *Whisper) Find() []Glob {
	panic("Find: unimplemented")
}
