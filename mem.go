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
	"github.com/wangjohn/quickselect"
)

type MetricID uint32
type Count uint32

// Whisper is an in-memory whisper-like store
type Whisper struct {
	sync.RWMutex
	t0 int32

	idx    int
	epochs []map[MetricID]Count

	midx    int
	minutes []map[MetricID]Count

	l *lookup
}

func NewWhisper(t0 int32, ecap, cap int, options ...func(*Whisper) error) *Whisper {

	t0 = t0 - (t0 % 60)

	epochs := make([]map[MetricID]Count, ecap)
	epochs[0] = make(map[MetricID]Count)

	minutes := make([]map[MetricID]Count, cap/60)
	minutes[0] = make(map[MetricID]Count)

	w := &Whisper{
		t0:      t0,
		epochs:  epochs,
		minutes: minutes,
		l:       newLookup(),
	}

	for _, o := range options {
		o(w)
	}

	return w
}

func TrigramCutoff(cutoff int) func(*Whisper) error {
	return func(w *Whisper) error {
		w.l.trigramCutoff = cutoff
		return nil
	}
}

func (w *Whisper) Set(t int32, metric string, val uint64) {

	count := Count(val)

	w.Lock()
	defer w.Unlock()

	// based on github.com/dgryski/go-timewindow

	if t == w.t0 {

		id := w.l.FindOrAdd(metric)

		m := w.epochs[w.idx]
		mm := w.minutes[w.midx]

		// have we seen this metric this epoch? Get the value so aggregate counts are correct
		v := m[id]

		// have we seen this metric this minute?
		_, ok := mm[id]
		if !ok {
			// one more occurrence of this metric
			w.l.AddRef(id)
		}

		m[id] = count
		mm[id] += count - v

		return
	}

	if t > w.t0 {
		// advance the buffer, clearing all maps we pass by

		for w.t0 < t {
			w.t0++
			w.idx++
			if w.idx >= len(w.epochs) {
				w.idx = 0
			}

			// TODO(dgryski): save the epoch map in a sync.Pool?
			w.epochs[w.idx] = nil

			if w.t0%60 == 0 {
				w.midx++
				if w.midx >= len(w.minutes) {
					w.midx = 0
				}

				mm := w.minutes[w.midx]
				for id, _ := range mm {
					w.l.DelRef(id)
				}
				w.minutes[w.midx] = nil
			}
		}

		id := w.l.FindOrAdd(metric)

		// TODO(dgryski): preallocate these maps to the size of one we just purged?
		w.epochs[w.idx] = map[MetricID]Count{id: count}

		if mm := w.minutes[w.midx]; mm == nil {
			w.l.AddRef(id)
			w.minutes[w.midx] = map[MetricID]Count{id: count}
		} else {
			_, ok := mm[id]
			if !ok {
				w.l.AddRef(id)
			}
			mm[id] += count
		}
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
		m = make(map[MetricID]Count)
		w.epochs[idx] = m
	}

	midx := w.midx - back/60
	if midx < 0 {
		midx += len(w.minutes)
	}

	mm := w.minutes[midx]
	if mm == nil {
		mm = make(map[MetricID]Count)
		w.minutes[midx] = mm
	}

	id := w.l.FindOrAdd(metric)

	v := m[id]

	_, ok := mm[id]
	if !ok {
		w.l.AddRef(id)
	}

	m[id] = count
	mm[id] += count - v
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

	// if from >= t0-len(w.epochs), we can handle it entirely at second resolution
	// otherwise, handle at minutely resolution

	step := int32(1)
	maps := w.epochs
	idx := w.idx

	if min := w.t0 - int32(len(maps)) + 1; from < min {
		// switch to minute resolution
		maps = w.minutes
		step = 60
		idx = w.midx

		// check that we're still in range
		if min = (w.t0 - w.t0%60) - int32(len(maps))*60 + 60; from < min {
			from = min
		}
	}

	idx -= int((w.t0 - from) / step)
	if idx < 0 {
		idx += len(maps)
	}

	if until > w.t0 {
		until = w.t0
	}

	from = from - (from % step)
	until = until - (until % step)

	points := (until - from + step) / step // inclusive of 'until'

	r := &Fetched{
		From:   from,
		Until:  until,
		Step:   step,
		Values: make([]float64, points),
	}

	l := len(maps)

	for p, t := 0, idx; p < int(points); p, t = p+1, t+1 {
		if t >= l {
			t = 0
		}

		m := maps[t]
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

func (g globByName) Len() int           { return len(g) }
func (g globByName) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g globByName) Less(i, j int) bool { return g[i].Metric < g[j].Metric }

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
		// only one trailing star
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

// sort by count, descending
type keysByCount struct {
	keys   []MetricID
	counts map[MetricID]Count
}

func (k keysByCount) Len() int           { return len(k.keys) }
func (k keysByCount) Swap(i, j int)      { k.keys[i], k.keys[j] = k.keys[j], k.keys[i] }
func (k keysByCount) Less(i, j int) bool { return k.counts[k.keys[i]] > k.counts[k.keys[j]] }

func (w *Whisper) TopK(prefix string, seconds int32) []Glob {

	w.RLock()
	defer w.RUnlock()

	glob := strings.Replace(prefix, ".", "/", -1) + ".wsp"

	buckets := int(seconds+59) / 60

	idx := w.midx
	l := len(w.minutes)

	idx -= buckets - 1
	if idx < 0 {
		idx += l
	}

	// gather counts for all metrics in this time period
	size := len(w.minutes[idx])
	matchingGlobs := make(map[MetricID]bool, size)
	counts := make(map[MetricID]Count, size)
	for i := 0; i < buckets; i++ {
		m := w.minutes[idx]
		for id, v := range m {
			var matched, ok bool
			if matched, ok = matchingGlobs[id]; !ok {
				k := w.l.Path(id)
				var err error
				matched, err = filepath.Match(glob, k)
				if err != nil {
					matched = false // make sure
				}
				matchingGlobs[id] = matched
			}
			if matched {
				counts[id] += v
			}
		}
		idx++
		if idx >= l {
			idx = 0
		}
	}

	var keys []MetricID
	for k, _ := range counts {
		keys = append(keys, k)
	}

	countedKeys := keysByCount{keys: keys, counts: counts}

	// TODO(dgryski): if keylen < countedKeys.Len(), don't bother with quickselect
	keylen := 100
	if keylen > countedKeys.Len() {
		keylen = countedKeys.Len()
	}

	quickselect.QuickSelect(countedKeys, keylen)

	countedKeys.keys = countedKeys.keys[:keylen]
	sort.Sort(countedKeys)

	var response []Glob

	for i := 0; i < keylen; i++ {
		m := w.l.Reverse(countedKeys.keys[i])
		response = append(response, Glob{Metric: m, IsLeaf: true})
	}

	return response
}

func (w *Whisper) Len() int {
	w.RLock()
	l := w.l.Len()
	w.RUnlock()
	return l
}

type lookup struct {
	// all metrics
	keys  map[string]MetricID
	revs  []string
	count int

	// currently 'active'
	active map[MetricID]int
	prefix *radix.Tree

	free []MetricID

	pathidx trigram.Index
	paths   []string

	// when do we stop indexing metrics with trigrams?
	trigramCutoff int
}

func newLookup() *lookup {
	return &lookup{
		keys: make(map[string]MetricID),

		active: make(map[MetricID]int),
		prefix: radix.New(),

		pathidx: trigram.NewIndex(nil),
	}
}

func (l *lookup) Len() int {
	return len(l.keys)
}

func (l *lookup) Find(key string) (MetricID, bool) {
	id, ok := l.keys[key]
	return id, ok
}

func (l *lookup) FindOrAdd(key string) MetricID {

	id, ok := l.keys[key]

	if ok {
		return id
	}

	useTrigramIndex := l.pathidx != nil && (l.trigramCutoff == 0 || len(l.keys) < l.trigramCutoff)

	path := strings.Replace(key, ".", "/", -1) + ".wsp"

	if len(l.free) == 0 || useTrigramIndex {
		id = MetricID(l.count)
		l.count++
		l.revs = append(l.revs, key)
		l.paths = append(l.paths, path)
	} else {
		id, l.free = l.free[0], l.free[1:]
		l.revs[id] = key
		l.paths[id] = path
	}

	l.keys[key] = id

	if useTrigramIndex {
		l.pathidx.Insert(path, trigram.DocID(id))
	} else {
		l.pathidx = nil
	}

	return id
}

func (l *lookup) Reverse(id MetricID) string {
	return l.revs[id]
}

func (l *lookup) Path(id MetricID) string {
	return l.paths[id]
}

func (l *lookup) AddRef(id MetricID) {
	v, ok := l.active[id]
	if !ok {
		l.prefix.Insert(l.revs[id], id)
	}

	l.active[id] = v + 1
}

func (l *lookup) DelRef(id MetricID) {
	l.active[id]--
	if l.active[id] == 0 {
		delete(l.active, id)
		delete(l.keys, l.revs[id])
		l.prefix.Delete(l.revs[id])
		l.revs[id] = ""
		if l.pathidx != nil {
			l.pathidx.Delete(l.paths[id], trigram.DocID(id))
		}
		l.paths[id] = ""
		l.free = append(l.free, id)
	}
}

func (l *lookup) Active(id MetricID) bool {
	return l.active[id] != 0
}

func (l *lookup) Prefix(query string, fn radix.WalkFn) {
	l.prefix.WalkPrefix(query, fn)
}

func (l *lookup) QueryPath(query string) []string {

	if l.pathidx == nil {
		return nil
	}

	fquery := query + ".wsp"

	ts := extractTrigrams(query)

	ids := l.pathidx.QueryTrigrams(ts)

	seen := make(map[string]bool)

	for _, id := range ids {

		p := l.paths[MetricID(id)]

		dir := filepath.Dir(p)

		if seen[dir] {
			continue
		}

		if matched, err := filepath.Match(query, dir); err == nil && matched {
			seen[dir] = true
			continue
		}

		if matched, err := filepath.Match(fquery, p); err == nil && matched {
			seen[p] = true
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
