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
	"time"

	"code.google.com/p/gogoprotobuf/proto"

	"github.com/dgryski/carbonmem"

	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
)

var Metrics *carbonmem.Whisper

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
	if prefix, seconds, ok = parseTopK(query); ok {
		topk = query[len(prefix):]
		globs = Metrics.TopK(prefix, seconds)
	} else {
		globs = Metrics.Find(query)
	}

	response := pb.GlobResponse{
		Name: &query,
	}

	var matches []*pb.GlobMatch
	for _, g := range globs {
		// fix up metric name
		m := pb.GlobMatch{
			Path:   proto.String(g.Metric + topk),
			IsLeaf: proto.Bool(g.IsLeaf),
		}
		matches = append(matches, &m)
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

	points := Metrics.Fetch(metric, int32(frint), int32(unint))

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

				Metrics.Set(int32(epoch), fields[0], uint64(count))
			}
		}(conn)
	}
}

func main() {

	wsize := flag.Int("w", 60, "window size")
	agg := flag.Int("a", 1, "aggregation period")
	epoch0 := flag.Int("epoch0", 0, "epoch0")
	port := flag.Int("p", 8001, "port to listen on (http)")
	gport := flag.Int("gp", 2003, "port to listen on (graphite)")

	flag.Parse()

	if *epoch0 == 0 {
		*epoch0 = int(time.Now().Unix())
	}

	Metrics = carbonmem.NewWhisper(int32(*epoch0), *wsize, int32(*agg))

	go graphiteServer(*gport)

	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", renderHandler)

	log.Println("http server starting on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
