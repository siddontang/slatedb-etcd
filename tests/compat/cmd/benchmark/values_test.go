package main

import (
	"encoding/json"
	"testing"
)

func TestBenchmarkValues(t *testing.T) {
	for _, pattern := range []string{"binary", "metadata"} {
		values, err := makeValues(pattern, 257, 10)
		if err != nil || len(values) != 10 || values[0] == values[1] {
			t.Fatalf("unexpected %s value pool", pattern)
		}
		for _, v := range values {
			if len(v) != 257 || (pattern == "metadata" && !json.Valid([]byte(v))) {
				t.Fatalf("invalid %s value", pattern)
			}
		}
	}
}
