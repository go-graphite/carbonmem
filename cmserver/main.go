// memory-backed carbon store: speaks graphite in, zipper out
// At the moment, shares a lot of code with grobian/carbonserver
package main

import (
	"bufio"
	"encoding/json"
	"expvar"
	"flag"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/gogoprotobuf/proto"

	"github.com/dgryski/carbonmem"

	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
)

var BuildVersion = "(development build)"

func parseTopK(query string) (string, int32, bool) {

	// prefix.blah.*.TopK.10m  => "prefix.blah.*", 600, true

	var idx int
	if idx = strings.Index(query, ".TopK."); idx == -1 {
		// not found
		return "", 0, false
	}

	prefix := query[:idx]

	timeIdx := idx + len(".TopK.")

	// look for number followed by 'm' or 's'
	unitsIdx := timeIdx
	for unitsIdx < len(query) && '0' <= query[unitsIdx] && query[unitsIdx] <= '9' {
		unitsIdx++
	}

	// ran off the end or no numbers present
	if unitsIdx == len(query) || unitsIdx == timeIdx {
		return "", 0, false
	}

	multiplier := 0
	switch query[unitsIdx] {
	case 's':
		multiplier = 1
	case 'm':
		multiplier = 60
	default:
		// unknown units
		return "", 0, false
	}

	if unitsIdx != len(query)-1 {
		return "", 0, false
	}

	timeUnits, err := strconv.Atoi(query[timeIdx:unitsIdx])
	if err != nil {
		return "", 0, false
	}

	return prefix, int32(timeUnits * multiplier), true
}

func findHandler(w http.ResponseWriter, req *http.Request) {

	query := req.FormValue("query")
	format := req.FormValue("format")

	if format != "json" && format != "protobuf" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var globs []carbonmem.Glob

	var topk string

	if strings.Count(query, ".") < Whispers.prefix {
		globs = Whispers.Glob(query)
	} else {
		if m := Whispers.Fetch(query); m != nil {
			if prefix, seconds, ok := parseTopK(query); ok {
				topk = query[len(prefix):]
				globs = m.TopK(prefix, seconds)
			} else {
				globs = m.Find(query)
			}
		}
	}

	response := pb.GlobResponse{
		Name: &query,
	}

	var matches []*pb.GlobMatch
	paths := make(map[string]struct{}, len(globs))
	for _, g := range globs {
		// fix up metric name
		metric := g.Metric + topk
		if _, ok := paths[metric]; !ok {
			m := pb.GlobMatch{
				Path:   proto.String(g.Metric + topk),
				IsLeaf: proto.Bool(g.IsLeaf),
			}
			matches = append(matches, &m)
			paths[metric] = struct{}{}
		}
	}

	response.Matches = matches

	var b []byte
	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		b, _ = json.Marshal(response)
	case "protobuf":
		w.Header().Set("Content-Type", "application/protobuf")
		b, _ = proto.Marshal(&response)
	}
	w.Write(b)
}

func renderHandler(w http.ResponseWriter, req *http.Request) {

	target := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	frint, _ := strconv.Atoi(from)
	unint, _ := strconv.Atoi(until)

	if format != "json" && format != "protobuf" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var metric string
	if prefix, _, ok := parseTopK(target); ok {
		metric = prefix
	} else {
		metric = target
	}

	metrics := Whispers.Fetch(metric)
	if metrics == nil {
		return
	}
	points := metrics.Fetch(metric, int32(frint), int32(unint))

	if points == nil {
		return
	}

	fromTime := int32(points.From)
	untilTime := int32(points.Until)
	step := int32(points.Step)
	response := pb.FetchResponse{
		Name:      &target,
		StartTime: &fromTime,
		StopTime:  &untilTime,
		StepTime:  &step,
		Values:    make([]float64, len(points.Values)),
		IsAbsent:  make([]bool, len(points.Values)),
	}

	for i, p := range points.Values {
		if math.IsNaN(p) {
			response.Values[i] = 0
			response.IsAbsent[i] = true
		} else {
			response.Values[i] = p
			response.IsAbsent[i] = false
		}
	}

	var b []byte
	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		b, _ = json.Marshal(response)
	case "protobuf":
		w.Header().Set("Content-Type", "application/protobuf")
		b, _ = proto.Marshal(&response)
	}
	w.Write(b)

}

func graphiteServer(port int) {

	ln, e := net.Listen("tcp", ":"+strconv.Itoa(port))

	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Println("graphite server starting on port", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go func(c net.Conn) {
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				// TODO(dgryski): under normal load, this is the only call that shows up in the profiles
				fields := strings.Fields(scanner.Text())
				if len(fields) != 3 {
					continue
				}

				// metric count epoch
				count, err := strconv.Atoi(fields[1])
				if err != nil {
					continue
				}

				epoch, err := strconv.Atoi(fields[2])
				if err != nil {
					continue
				}

				metrics := Whispers.FetchOrCreate(fields[0])

				metrics.Set(int32(epoch), fields[0], uint64(count))
			}
		}(conn)
	}
}

type whispers struct {
	sync.RWMutex
	metrics map[string]*carbonmem.Whisper

	windowSize int
	epochSize  int
	epoch0     int
	prefix     int
}

var Whispers whispers

func findNodePrefix(prefix int, metric string) string {

	var found int
	for i, c := range metric {
		if c == '.' {
			found++
			if found >= prefix {
				return metric[:i]
			}
		}
	}
	return metric
}

func (w *whispers) FetchOrCreate(metric string) *carbonmem.Whisper {

	m := w.Fetch(metric)

	if m == nil {
		prefix := findNodePrefix(w.prefix, metric)
		var ok bool
		w.Lock()
		m, ok = w.metrics[prefix]
		if !ok {
			m = carbonmem.NewWhisper(int32(w.epoch0), w.epochSize, w.windowSize)
			w.metrics[prefix] = m
		}
		w.Unlock()
	}

	return m
}

func (w *whispers) Fetch(metric string) *carbonmem.Whisper {
	prefix := findNodePrefix(w.prefix, metric)

	w.RLock()
	m := w.metrics[prefix]
	w.RUnlock()

	return m
}

func (w *whispers) Glob(query string) []carbonmem.Glob {

	query = strings.Replace(query, ".", "/", -1)
	slashes := strings.Count(query, "/")

	w.RLock()
	var glob []carbonmem.Glob
	for m := range w.metrics {
		qm := strings.Replace(m, ".", "/", slashes)
		if trim := strings.Index(qm, "."); trim != -1 {
			qm = qm[:trim]
			m = m[:trim]
		}
		if match, err := filepath.Match(query, qm); err == nil && match {
			glob = append(glob, carbonmem.Glob{Metric: m})
		}
	}

	w.RUnlock()

	return glob
}

func main() {

	Whispers = whispers{metrics: make(map[string]*carbonmem.Whisper)}

	flag.IntVar(&Whispers.windowSize, "w", 600, "window size")
	flag.IntVar(&Whispers.epochSize, "e", 60, "epoch window size")
	flag.IntVar(&Whispers.epoch0, "epoch0", 0, "epoch0")
	flag.IntVar(&Whispers.prefix, "prefix", 0, "prefix nodes to shard on")

	port := flag.Int("p", 8001, "port to listen on (http)")
	gport := flag.Int("gp", 2003, "port to listen on (graphite)")

	flag.Parse()

	expvar.NewString("BuildVersion").Set(BuildVersion)
	log.Println("starting carbonmem", BuildVersion)

	if Whispers.epoch0 == 0 {
		Whispers.epoch0 = int(time.Now().Unix())
	}

	go graphiteServer(*gport)

	http.HandleFunc("/metrics/find/", accessHandler(findHandler))
	http.HandleFunc("/render/", accessHandler(renderHandler))

	log.Println("http server starting on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func accessHandler(handler func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		handler(w, r)
		since := time.Since(t0)
		log.Println(r.RequestURI, since.Nanoseconds()/int64(time.Millisecond))
	}
}
