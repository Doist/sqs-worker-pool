package main

import (
	"reflect"
	"regexp"
	"testing"
)

func TestFilterQueues(t *testing.T) {
	input := []struct {
		s      string
		passes bool
	}{
		{"prefix/foo1", true},
		{"prefix/foo2", false},
		{"prefix/bar1", true},
		{"prefix/baz1", false},
	}
	queues := make([]string, 0, len(input))
	for _, x := range input {
		queues = append(queues, x.s)
	}
	var want []string
	for _, x := range input {
		if x.passes {
			want = append(want, x.s)
		}
	}
	include := regexp.MustCompile(`(foo|bar)\d`)
	exclude := regexp.MustCompile(`\w+2`)
	out := filterQueues(queues, include, exclude)
	if !reflect.DeepEqual(out, want) {
		t.Logf(" got:\t%q", out)
		t.Logf("want:\t%q", want)
		t.Fatal("unexpected output")
	}
}
