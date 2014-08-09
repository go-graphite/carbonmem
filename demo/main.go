// memory-backed carbon store: speaks graphite in, zipper out
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/dgryski/carbonmem"

	"github.com/dustin/go-humanize"
	cspb "github.com/grobian/carbonserver/carbonserverpb"
)

var _ = cspb.FetchResponse{}
var _ = cspb.GlobResponse{}

func main() {

	file := flag.String("f", "", "input file")
	wsize := flag.Int("w", 60, "window size")
	epoch0 := flag.Int("epoch0", 0, "epoch0")

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

		w.Incr(int32(t), fields[1], 1)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Println(humanize.Bytes(m.Alloc))
}
