package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"code.google.com/p/gogoprotobuf/proto"

	"github.com/davecgh/go-spew/spew"
	"github.com/dgryski/carbonmem"
	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
)

func TestParseTopK(t *testing.T) {

	var tests = []struct {
		query   string
		prefix  string
		seconds int32
		ok      bool
	}{
		// good
		{"prefix.blah.*.TopK.10m", "prefix.blah.*", 10 * 60, true},
		{"prefix.blah.*.TopK.10s", "prefix.blah.*", 10, true},
		{"prefix.blah.foo.TopK.10m", "prefix.blah.foo", 600, true},

		// bad
		{"prefix.foo.bar.baz", "", 0, false},
		{"prefix.blah.TopK.10m*", "", 0, false},
		{"prefix.blah.TopK.10", "", 0, false},
		{"prefix.blah.TopK.10m.", "", 0, false},
		{"prefix.blah.TopK.10z", "", 0, false},
		{"prefix.blah.TopK.10m.*.foo", "", 0, false},
		{"prefix.blah.TopK.10m.foo.*", "", 0, false},
		{"prefix.blah.TopK.10s*", "", 0, false},
		{"prefix.blah.TopK.-10m.*", "", 0, false},
		{"prefix.blah.TopK.*", "", 0, false},
		{"prefix.blah.TopK.", "", 0, false},
	}

	for _, tt := range tests {
		if prefix, seconds, ok := parseTopK(tt.query); prefix != tt.prefix || seconds != tt.seconds || ok != tt.ok {
			t.Errorf("parseTopK(%s)=(%q,%v,%v), want (%q,%v,%v)", tt.query, prefix, seconds, ok, tt.prefix, tt.seconds, tt.ok)
		}
	}
}

func TestTopKFind(t *testing.T) {

	Metrics = carbonmem.NewWhisper(100, 10)

	for _, m := range []struct {
		epoch  int32
		metric string
		count  uint64
	}{
		{100, "foo.bar", 10},
		{100, "foo.baz", 50},
		{101, "foo.bar", 11},
		{101, "foo.baz", 40},

		{102, "foo.bar", 12},
		{103, "foo.bar", 13},
		{103, "foo.qux", 13},
		{104, "foo.bar", 14},
	} {
		Metrics.Set(m.epoch, m.metric, m.count)
	}

	for _, tt := range []struct {
		query string
		want  pb.GlobResponse
	}{
		{
			"foo.*",
			pb.GlobResponse{
				Name: proto.String("foo.*"),
				Matches: []*pb.GlobMatch{
					&pb.GlobMatch{Path: proto.String("foo.bar"), IsLeaf: proto.Bool(true)},
					&pb.GlobMatch{Path: proto.String("foo.baz"), IsLeaf: proto.Bool(true)},
					&pb.GlobMatch{Path: proto.String("foo.qux"), IsLeaf: proto.Bool(true)},
				},
			},
		},

		{
			"foo.*.TopK.3s",
			pb.GlobResponse{
				Name: proto.String("foo.*.TopK.3s"),
				Matches: []*pb.GlobMatch{
					&pb.GlobMatch{Path: proto.String("foo.bar.TopK.3s"), IsLeaf: proto.Bool(true)},
					&pb.GlobMatch{Path: proto.String("foo.qux.TopK.3s"), IsLeaf: proto.Bool(true)},
				},
			},
		},

		{
			"foo.*.TopK.5s",
			pb.GlobResponse{
				Name: proto.String("foo.*.TopK.5s"),
				Matches: []*pb.GlobMatch{
					&pb.GlobMatch{Path: proto.String("foo.baz.TopK.5s"), IsLeaf: proto.Bool(true)},
					&pb.GlobMatch{Path: proto.String("foo.bar.TopK.5s"), IsLeaf: proto.Bool(true)},
					&pb.GlobMatch{Path: proto.String("foo.qux.TopK.5s"), IsLeaf: proto.Bool(true)},
				},
			},
		},
	} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("/metrics/find/?query=%s&format=json", tt.query), nil)

		w := httptest.NewRecorder()

		findHandler(w, req)

		var response pb.GlobResponse

		json.Unmarshal(w.Body.Bytes(), &response)

		if !reflect.DeepEqual(response, tt.want) {
			t.Errorf("Find(%s)=%#v, want %#v", tt.query, spew.Sdump(response), spew.Sdump(tt.want))
		}
	}
}

func TestTopKRender(t *testing.T) {

	Metrics = carbonmem.NewWhisper(100, 10)

	for _, m := range []struct {
		epoch  int32
		metric string
		count  uint64
	}{
		{100, "foo.bar", 10},
		{100, "foo.baz", 50},
		{101, "foo.bar", 11},
		{101, "foo.baz", 40},

		{102, "foo.bar", 12},
		{103, "foo.bar", 13},
		{103, "foo.qux", 13},
		{104, "foo.bar", 14},
	} {
		Metrics.Set(m.epoch, m.metric, m.count)
	}

	for _, tt := range []struct {
		target string
		from   int32
		until  int32
		want   pb.FetchResponse
	}{
		{
			"foo.bar",
			100, 104,
			pb.FetchResponse{
				Name:      proto.String("foo.bar"),
				StartTime: proto.Int32(100),
				StopTime:  proto.Int32(104),
				StepTime:  proto.Int32(1),
				Values:    []float64{10, 11, 12, 13, 14},
				IsAbsent:  []bool{false, false, false, false, false},
			},
		},

		{
			"foo.bar.TopK.3s",
			100, 104,
			pb.FetchResponse{
				Name:      proto.String("foo.bar.TopK.3s"),
				StartTime: proto.Int32(100),
				StopTime:  proto.Int32(104),
				StepTime:  proto.Int32(1),
				Values:    []float64{10, 11, 12, 13, 14},
				IsAbsent:  []bool{false, false, false, false, false},
			},
		},
	} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("/render/?target=%s&from=%d&until=%d&format=json", tt.target, tt.from, tt.until), nil)

		w := httptest.NewRecorder()

		renderHandler(w, req)

		var response pb.FetchResponse

		json.Unmarshal(w.Body.Bytes(), &response)

		if !reflect.DeepEqual(response, tt.want) {
			t.Errorf("Render(%s,%d,%d)=%#v, want %#v", tt.target, tt.from, tt.until, spew.Sdump(response), spew.Sdump(tt.want))
		}
	}
}
