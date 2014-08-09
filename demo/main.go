// memory-backed carbon store: speaks graphite in, zipper out
// At the moment, shares a lot of code with grobian/carbonserver
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/goprotobuf/proto"

	"github.com/dgryski/carbonmem"

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
					log.Println("error splitting line: ", scanner.Text())
					continue
				}

				// metric count epoch
				count, err := strconv.Atoi(fields[1])
				if err != nil {
					log.Printf("error parsing count %s: %s\n", fields[1], err)
					continue
				}

				epoch, err := strconv.Atoi(fields[2])
				if err != nil {
					log.Printf("error parsing epoch %s: %s\n", fields[1], err)
					continue
				}

				Metrics.Set(int32(epoch), fields[0], uint64(count))

			}
		}(conn)
	}
}

func main() {

	wsize := flag.Int("w", 60, "window size")
	epoch0 := flag.Int("epoch0", 0, "epoch0")
	port := flag.Int("p", 8001, "port to listen on (http)")
	gport := flag.Int("gp", 2003, "port to listen on (graphite)")

	flag.Parse()

	if *epoch0 == 0 {
		*epoch0 = int(time.Now().Unix())
	}

	Metrics = carbonmem.NewWhisper(int32(*epoch0), *wsize)

	go graphiteServer(*gport)

	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", renderHandler)

	log.Println("http server starting on port", port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
