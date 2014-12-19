// memory-backed carbon store: speaks graphite in, zipper out
// At the moment, shares a lot of code with grobian/carbonserver
package main

import (
	"bufio"
	"encoding/json"
	_ "expvar"
	"flag"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/gogoprotobuf/proto"

	"github.com/dgryski/carbonmem"

	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
)

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

	var prefix, topk string
	var seconds int32
	var ok bool

	var whispers []*carbonmem.Whisper

	if strings.Count(query, ".") < metricConfig.prefix {
		for _, m := range Metrics {
			whispers = append(whispers, m)
		}
	} else {
		if m := whisperFetch(metricConfig.prefix, query); m != nil {
			whispers = append(whispers, m)
		}
	}

	for _, metrics := range whispers {
		if prefix, seconds, ok = parseTopK(query); ok {
			topk = query[len(prefix):]
			globs = append(globs, metrics.TopK(prefix, seconds)...)
		} else {
			globs = append(globs, metrics.Find(query)...)
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

	metrics := whisperFetch(metricConfig.prefix, metric)
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

				metrics := whisperFetchOrCreate(metricConfig.prefix, fields[0])

				metrics.Set(int32(epoch), fields[0], uint64(count))
			}
		}(conn)
	}
}

var metricsLock sync.RWMutex
var Metrics map[string]*carbonmem.Whisper

var metricConfig struct {
	windowSize int
	epochSize  int
	epoch0     int
	prefix     int
}

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

func whisperFetchOrCreate(nprefix int, metric string) *carbonmem.Whisper {

	m := whisperFetch(nprefix, metric)

	if m == nil {
		prefix := findNodePrefix(nprefix, metric)
		var ok bool
		metricsLock.Lock()
		m, ok = Metrics[prefix]
		if !ok {
			m = carbonmem.NewWhisper(int32(metricConfig.epoch0), metricConfig.epochSize, metricConfig.windowSize)
			Metrics[prefix] = m
		}
		metricsLock.Unlock()
	}

	return m
}

func whisperFetch(nprefix int, metric string) *carbonmem.Whisper {
	prefix := findNodePrefix(nprefix, metric)

	metricsLock.RLock()
	m := Metrics[prefix]
	metricsLock.RUnlock()

	return m
}

func main() {

	flag.IntVar(&metricConfig.windowSize, "w", 600, "window size")
	flag.IntVar(&metricConfig.epochSize, "e", 60, "epoch window size")
	flag.IntVar(&metricConfig.epoch0, "epoch0", 0, "epoch0")
	flag.IntVar(&metricConfig.prefix, "prefix", 0, "prefix nodes to shard on")

	port := flag.Int("p", 8001, "port to listen on (http)")
	gport := flag.Int("gp", 2003, "port to listen on (graphite)")

	flag.Parse()

	if metricConfig.epoch0 == 0 {
		metricConfig.epoch0 = int(time.Now().Unix())
	}

	Metrics = make(map[string]*carbonmem.Whisper)

	go graphiteServer(*gport)

	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", renderHandler)

	log.Println("http server starting on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
