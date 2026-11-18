// Generate benchmark values before measurement so the client can reuse them.
package main

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func makeValues(pattern string, size, count int) ([]string, error) {
	if pattern == "repeat" {
		return []string{strings.Repeat("v", size)}, nil
	}
	if pattern != "binary" && pattern != "metadata" {
		return nil, fmt.Errorf("unknown value pattern %q", pattern)
	}
	if pattern == "metadata" && size < 256 {
		return nil, fmt.Errorf("metadata values require at least 256 bytes")
	}
	// Limit client memory for large values. Reuse the pool when the workload is longer.
	count = min(count, max(1, (32*1024*1024)/size))
	values := make([]string, count)
	for i := range values {
		if pattern == "metadata" {
			const record = `{"kind":"Member","apiVersion":"v1",` +
				`"metadata":{"name":"member-%d","labels":{"cluster":"bench","shard":"%d"}},` +
				`"spec":{"address":"10.0.%d.%d","capacity":%d,"status":"active",` +
				`"note":"simulated metadata"}}`
			body := fmt.Sprintf(record, i, i%17, i%251, (i/251)%251, i*12)
			values[i] = body + strings.Repeat(" ", size-len(body))
			continue
		}
		buf := make([]byte, ((size+7)/8)*8)
		for j := 0; j < size; j += 8 {
			binary.LittleEndian.PutUint64(buf[j:], mix(uint64(i)<<32|uint64(j)))
		}
		values[i] = string(buf[:size])
	}
	return values, nil
}
