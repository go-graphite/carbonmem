// memory-backed carbon store: speaks graphite in, zipper out
// At the moment, shares a lot of code with grobian/carbonserver
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"code.google.com/p/goprotobuf/proto"

	"github.com/dgryski/carbonmem"

	"github.com/dustin/go-humanize"
	cspb "github.com/grobian/carbonserver/carbonserverpb"
)

var Metrics *carbonmem.Whisper

func findHandler(w http.ResponseWriter, req *http.Request) {

	target := req.FormValue("target")
	format := req.FormValue("format")

	globs := Metrics.Find(target)

	if format != "json" && format != "protobuf" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	response := cspb.GlobResponse{
		Name: &target,
	}

	var matches []*cspb.GlobMatch
	for _, g := range globs {
		m := cspb.GlobMatch{
			Path:   proto.String(g.Metric),
			IsLeaf: proto.Bool(g.IsLeaf),
		}
		matches = append(matches, &m)
	}

	response.Matches = matches

	var b []byte
	switch format {
	case "json":
		b, _ = json.Marshal(response)
	case "protobuf":
		b, _ = proto.Marshal(&response)
	}
	w.Write(b)
}

func renderHandler(w http.ResponseWriter, req *http.Request) {

	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	frint, _ := strconv.Atoi(from)
	unint, _ := strconv.Atoi(until)

	points := Metrics.Fetch(metric, int32(frint), int32(unint))

	if points == nil {
		log.Println("points is nil")
		return
	}

	if format != "json" && format != "protobuf" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	fromTime := int32(points.From)
	untilTime := int32(points.Until)
	step := int32(points.Step)
	response := cspb.FetchResponse{
		Name:      &metric,
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
		b, _ = json.Marshal(response)
	case "protobuf":
		b, _ = proto.Marshal(&response)
	}
	w.Write(b)

}

func main() {

	file := flag.String("f", "", "input file")
	wsize := flag.Int("w", 60, "window size")
	epoch0 := flag.Int("epoch0", 0, "epoch0")
	port := flag.Int("p", 8001, "port to listen on")

	flag.Parse()

	if *file == "" {
		log.Fatal("no input file given")
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(f)

	w := carbonmem.NewWhisper(int32(*epoch0), *wsize)

	var lines int

	for scanner.Scan() {
		line := scanner.Text()
		lines++
		fields := strings.Split(line, "\t")

		t, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Println("skipping ", fields[0])
			continue
		}

		if lines%(1<<20) == 0 {
			log.Println("processed", lines)
		}

		w.Set(int32(t), fields[1], 1)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Println(humanize.Bytes(m.Alloc))

	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", renderHandler)

	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
}
