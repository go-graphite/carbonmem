// memory-backed carbon store: speaks graphite in, zipper out
package carbonmem

import (
	"math"

	"sort"
	"strings"
	"sync"
)

// Whisper is an in-memory whisper-like store
type Whisper struct {
	sync.Mutex
	t0     int32
	agg    int32
	idx    int
	epochs []map[int]uint64
	locks  []sync.Mutex

	// TODO(dgryski): move this to armon/go-radix to speed up prefix matching
	known map[int]int // metric -> #epochs it appears in

	l *lookup
}

func NewWhisper(t0 int32, cap int, agg int32) *Whisper {

	t0 = t0 - (t0 % agg)

	epochs := make([]map[int]uint64, cap)
	epochs[0] = make(map[int]uint64)

	return &Whisper{
		t0:     t0,
		agg:    agg,
		epochs: epochs,
		known:  make(map[int]int),
		locks:  make([]sync.Mutex, cap),
		l:      newLookup(),
	}
}

func (w *Whisper) Set(t int32, metric string, val uint64) {

	w.Lock()
	defer w.Unlock()

	// based on github.com/dgryski/go-timewindow

	t = t - (t % w.agg)

	if t == w.t0 {

		id := w.l.FindOrAdd(metric)

		w.locks[w.idx].Lock()

		m := w.epochs[w.idx]

		// have we seen this metric this epoch?
		_, ok := m[id]
		if !ok {
			// one more occurrence of this metric
			w.known[id]++
		}

		m[id] = val
		w.locks[w.idx].Unlock()
		return
	}

	if t > w.t0 {
		// advance the buffer, decrementing counts for all entries in the
		// maps we pass by

		for w.t0 < t {
			w.t0 += w.agg
			w.idx++
			if w.idx >= len(w.epochs) {
				w.idx = 0
			}

			w.locks[w.idx].Lock()
			m := w.epochs[w.idx]
			if m != nil {
				for id, _ := range m {
					w.known[id]--
					if w.known[id] == 0 {
						delete(w.known, id)
					}
				}
				w.epochs[w.idx] = nil
			}
			w.locks[w.idx].Unlock()
		}

		id := w.l.FindOrAdd(metric)

		w.known[id]++
		w.locks[w.idx].Lock()
		w.epochs[w.idx] = map[int]uint64{id: val}
		w.locks[w.idx].Unlock()
		return
	}

	// TODO(dgryski): limit how far back we can update

	// less common -- update the past
	back := int((w.t0 - t) / w.agg)

	if back >= len(w.epochs) {
		// too far in the past, ignore
		return
	}

	idx := w.idx - back

	if idx < 0 {
		// need to wrap around
		idx += len(w.epochs)
	}

	w.locks[idx].Lock()
	m := w.epochs[idx]
	if m == nil {
		m = make(map[int]uint64)
		w.epochs[idx] = m
	}

	id := w.l.FindOrAdd(metric)

	_, ok := m[id]
	if !ok {
		w.known[id]++
	}
	m[id] = val
	w.locks[idx].Unlock()
}

type Fetched struct {
	From   int32
	Until  int32
	Step   int32
	Values []float64
}

func (w *Whisper) Fetch(metric string, from int32, until int32) *Fetched {

	w.Lock()

	// round to window
	from = from - (from % w.agg)
	until = until - (until % w.agg)

	if from > w.t0 {
		w.Unlock()
		return nil
	}

	id, ok := w.l.Find(metric)
	if !ok {
		// unknown metric
		w.Unlock()
		return nil
	}

	if _, ok := w.known[id]; !ok {
		w.Unlock()
		return nil
	}

	if until < from {
		w.Unlock()
		return nil
	}

	if min := w.t0 - int32(len(w.epochs))*w.agg + 1; from < min {
		from = min
	}

	idx := w.idx - int((w.t0-from)/w.agg)
	if idx < 0 {
		idx += len(w.epochs)
	}

	points := (until - from + (w.agg - 1) + 1) / w.agg // inclusive of 'until'
	r := &Fetched{
		From:   from,
		Until:  until,
		Step:   w.agg,
		Values: make([]float64, points),
	}

	l := len(w.epochs)

	w.Unlock()

	for p, t := 0, idx; p < int(points); p, t = p+1, t+1 {
		if t >= l {
			t = 0
		}

		w.locks[t].Lock()

		m := w.epochs[t]
		if v, ok := m[id]; ok {
			r.Values[p] = float64(v)
		} else {
			r.Values[p] = math.NaN()
		}
		w.locks[t].Unlock()
	}

	return r
}

type Glob struct {
	Metric string
	IsLeaf bool
}

type globByName []Glob

func (g globByName) Len() int {
	return len(g)
}

func (g globByName) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func (g globByName) Less(i, j int) bool {
	return g[i].Metric < g[j].Metric
}

// TODO(dgryski): only does prefix matching for the moment

func (w *Whisper) Find(query string) []Glob {

	w.Lock()
	defer w.Unlock()

	query = strings.TrimSuffix(query, "*")

	var response []Glob
	l := len(query)
	for id, _ := range w.known {
		k := w.l.Reverse(id)
		if strings.HasPrefix(k, query) {
			// figure out if we're a leaf or not
			dot := strings.IndexByte(k[l:], '.')
			var leaf bool
			m := k
			if dot == -1 {
				leaf = true
			} else {
				m = k[:dot+l]
			}
			response = appendIfUnique(response, Glob{Metric: m, IsLeaf: leaf})
		}
	}

	sort.Sort(globByName(response))

	return response
}

type keysByCount struct {
	keys   []int
	counts map[int]uint64
}

func (k keysByCount) Len() int {
	return len(k.keys)
}

func (k keysByCount) Swap(i, j int) {
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
}

func (k keysByCount) Less(i, j int) bool {
	// actually "GreaterThan"
	return k.counts[k.keys[i]] > k.counts[k.keys[j]]
}

func (w *Whisper) TopK(prefix string, seconds int32) []Glob {

	buckets := int(seconds / w.agg)

	w.Lock()
	idx := w.idx
	l := len(w.epochs)
	w.Unlock()

	idx -= buckets - 1
	if idx < 0 {
		idx += l
	}

	// gather counts for all metrics in this time period
	counts := make(map[int]uint64)
	for i := 0; i < buckets; i++ {
		w.locks[idx].Lock()
		m := w.epochs[idx]
		for id, v := range m {
			k := w.l.Reverse(id)
			if strings.HasPrefix(k, prefix) {
				counts[id] += v
			}
		}
		w.locks[idx].Unlock()
		idx++
		if idx >= l {
			idx = 0
		}
	}

	var keys []int
	for k, _ := range counts {
		keys = append(keys, k)
	}

	countedKeys := keysByCount{keys: keys, counts: counts}

	sort.Sort(countedKeys)
	var response []Glob

	for i := 0; i < 100 && i < countedKeys.Len(); i++ {
		m := w.l.Reverse(countedKeys.keys[i])
		response = append(response, Glob{Metric: m, IsLeaf: true})
	}

	return response
}

// TODO(dgryski): replace with something faster if needed

func appendIfUnique(response []Glob, g Glob) []Glob {

	for i := range response {
		if response[i].Metric == g.Metric {
			return response
		}
	}

	return append(response, g)
}

type lookup struct {
	keys    map[string]int
	revKeys map[int]string
	numKeys int
}

func newLookup() *lookup {
	return &lookup{
		keys:    make(map[string]int),
		revKeys: make(map[int]string),
	}
}

func (l *lookup) Find(key string) (int, bool) {
	id, ok := l.keys[key]
	return id, ok
}

func (l *lookup) FindOrAdd(key string) int {

	id, ok := l.keys[key]

	if ok {
		return id
	}

	id = l.numKeys
	l.numKeys++

	l.keys[key] = id
	l.revKeys[id] = key

	return id
}

func (l *lookup) Reverse(id int) string {

	key, ok := l.revKeys[id]

	if !ok {
		panic("looked up invalid key")
	}

	return key
}
