package main

import (
	"testing"
)

func TestParseTopK(t *testing.T) {

	var tests = []struct {
		query   string
		prefix  string
		seconds int
		ok      bool
	}{
		// good
		{"prefix.blah.TopK.10m.*", "prefix.blah.", 10 * 60, true},
		{"prefix.blah.TopK.10s.*", "prefix.blah.", 10, true},

		// bad
		{"prefix.foo.bar.baz", "", 0, false},
		{"prefix.blah.TopK.10m*", "", 0, false},
		{"prefix.blah.TopK.10", "", 0, false},
		{"prefix.blah.TopK.10m", "", 0, false},
		{"prefix.blah.TopK.10z", "", 0, false},
		{"prefix.blah.TopK.10m.foo", "", 0, false},
		{"prefix.blah.TopK.10m.*.foo", "", 0, false},
		{"prefix.blah.TopK.10m.foo.*", "", 0, false},
		{"prefix.blah.TopK.10s*", "", 0, false},
		{"prefix.blah.TopK.-10m.*", "", 0, false},
		{"prefix.blah.TopK.*", "", 0, false},
		{"prefix.blah.TopK.", "", 0, false},
	}

	for _, tt := range tests {
		if prefix, seconds, ok := parseTopK(tt.query); prefix != tt.prefix || seconds != tt.seconds || ok != tt.ok {
			t.Errorf("parseTopK(%s)=(%v,%v,%v), want %v,%v,%v", tt.query, prefix, seconds, ok, tt.prefix, tt.seconds, tt.ok)
		}
	}
}
