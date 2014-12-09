// memory-backed carbon store: speaks graphite in, zipper out
package carbonmem

import (
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/armon/go-radix"
	"github.com/dgryski/go-trigram"
)

// Whisper is an in-memory whisper-like store
type Whisper struct {
	sync.RWMutex
	t0     int32
	agg    int32
	idx    int
	epochs []map[int]uint64

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

		m := w.epochs[w.idx]

		// have we seen this metric this epoch?
		_, ok := m[id]
		if !ok {
			// one more occurrence of this metric
			w.l.AddRef(id)
		}

		m[id] = val
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

			m := w.epochs[w.idx]
			if m != nil {
				for id, _ := range m {
					w.l.DelRef(id)
				}
				w.epochs[w.idx] = nil
			}
		}

		id := w.l.FindOrAdd(metric)

		w.l.AddRef(id)

		w.epochs[w.idx] = map[int]uint64{id: val}
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

	m := w.epochs[idx]
	if m == nil {
		m = make(map[int]uint64)
		w.epochs[idx] = m
	}

	id := w.l.FindOrAdd(metric)

	_, ok := m[id]
	if !ok {
		w.l.AddRef(id)
	}
	m[id] = val
}

type Fetched struct {
	From   int32
	Until  int32
	Step   int32
	Values []float64
}

func (w *Whisper) Fetch(metric string, from int32, until int32) *Fetched {

	w.RLock()
	defer w.RUnlock()

	// round to window
	from = from - (from % w.agg)
	until = until - (until % w.agg)

	if from > w.t0 {
		return nil
	}

	id, ok := w.l.Find(metric)
	if !ok {
		// unknown metric
		return nil
	}

	if !w.l.Active(id) {
		return nil
	}

	if until < from {
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

	for p, t := 0, idx; p < int(points); p, t = p+1, t+1 {
		if t >= l {
			t = 0
		}

		m := w.epochs[t]
		if v, ok := m[id]; ok {
			r.Values[p] = float64(v)
		} else {
			r.Values[p] = math.NaN()
		}
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

// TODO(dgryski): this needs most of the logic in grobian/carbsonerver:findHandler()

func (w *Whisper) Find(query string) []Glob {

	w.RLock()
	defer w.RUnlock()

	// no wildcard == exact match only
	var star int
	if star = strings.Index(query, "*"); star == -1 {
		if _, ok := w.l.Find(query); !ok {
			return nil
		}
		return []Glob{{Metric: query, IsLeaf: true}}
	}

	var response []Glob

	if star == len(query)-1 {
		query = strings.TrimSuffix(query, "*")

		l := len(query)
		seen := make(map[string]bool)
		w.l.Prefix(query, func(k string, v interface{}) bool {
			// figure out if we're a leaf or not
			dot := strings.IndexByte(k[l:], '.')
			var leaf bool
			m := k
			if dot == -1 {
				leaf = true
			} else {
				m = k[:dot+l]
			}
			if !seen[m] {
				seen[m] = true
				response = append(response, Glob{Metric: m, IsLeaf: leaf})
			}
			// false == "don't terminate iteration"
			return false
		})
	} else {
		// at least one interior star

		query = strings.Replace(query, ".", "/", -1)

		paths := w.l.QueryPath(query)

		for _, p := range paths {
			m := strings.Replace(p, "/", ".", -1)
			var leaf bool
			if strings.HasSuffix(p, ".wsp") {
				m = strings.TrimSuffix(m, ".wsp")
				leaf = true
			}
			response = append(response, Glob{Metric: m, IsLeaf: leaf})
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

	w.RLock()
	defer w.RUnlock()

	buckets := int(seconds / w.agg)

	idx := w.idx
	l := len(w.epochs)

	idx -= buckets - 1
	if idx < 0 {
		idx += l
	}

	// gather counts for all metrics in this time period
	counts := make(map[int]uint64)
	for i := 0; i < buckets; i++ {
		m := w.epochs[idx]
		for id, v := range m {
			k := w.l.Reverse(id)
			if strings.HasPrefix(k, prefix) {
				counts[id] += v
			}
		}
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

type lookup struct {
	// all metrics
	keys  map[string]int
	rev   map[int]string
	count int

	// currently 'active'
	active map[int]int
	prefix *radix.Tree

	pathidx trigram.Index
	paths   []string
}

func newLookup() *lookup {
	return &lookup{
		keys: make(map[string]int),
		rev:  make(map[int]string),

		active: make(map[int]int),
		prefix: radix.New(),

		pathidx: trigram.NewIndex(nil),
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

	id = l.count
	l.count++

	l.keys[key] = id
	l.rev[id] = key

	path := strings.Replace(key, ".", "/", -1) + ".wsp"

	l.pathidx.Insert(path, trigram.DocID(id))
	l.paths = append(l.paths, path)

	return id
}

func (l *lookup) Reverse(id int) string {

	key, ok := l.rev[id]

	if !ok {
		panic("looked up invalid key")
	}

	return key
}

func (l *lookup) AddRef(id int) {
	v, ok := l.active[id]
	if !ok {
		l.prefix.Insert(l.rev[id], id)
	}

	l.active[id] = v + 1
}

func (l *lookup) DelRef(id int) {
	l.active[id]--
	if l.active[id] == 0 {
		delete(l.active, id)
		l.prefix.Delete(l.rev[id])
	}
}

func (l *lookup) Active(id int) bool {
	return l.active[id] != 0
}

func (l *lookup) Prefix(query string, fn radix.WalkFn) {
	l.prefix.WalkPrefix(query, fn)
}

func (l *lookup) QueryPath(query string) []string {

	var fquery string

	if !strings.HasSuffix(query, "*") {
		fquery = query + ".wsp"
	}

	ts := extractTrigrams(query)

	ids := l.pathidx.QueryTrigrams(ts)

	seen := make(map[string]bool)

	for _, id := range ids {

		p := l.paths[int(id)]

		dir := filepath.Dir(p)

		if matched, err := filepath.Match(query, dir); err == nil && matched {
			seen[dir] = true
			continue
		}

		if fquery != "" {
			if matched, err := filepath.Match(fquery, p); err == nil && matched {
				seen[p] = true
			}
		}
	}

	var files []string

	for p := range seen {
		files = append(files, p)
	}

	return files
}

func extractTrigrams(query string) []trigram.T {

	if len(query) < 3 {
		return nil
	}

	var start int
	var i int

	var trigrams []trigram.T

	for i < len(query) {
		if query[i] == '[' || query[i] == '*' || query[i] == '?' {
			trigrams = trigram.Extract(query[start:i], trigrams)

			if query[i] == '[' {
				for i < len(query) && query[i] != ']' {
					i++
				}
			}

			start = i + 1
		}
		i++
	}

	trigrams = trigram.Extract(query[start:i], trigrams)

	return trigrams
}
