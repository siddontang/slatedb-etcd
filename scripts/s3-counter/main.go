// Command s3-counter is a reverse proxy that counts S3 requests and bytes by bucket and API.
//
// scripts/bench-shared-state.py puts it in front of a local RustFS server. The proxy keeps the
// Host header of each request, so AWS Signature Version 4 signatures stay valid.
// GET /__counts returns the counts as JSON: {bucket: {api: [calls, bytes in, bytes out]}}.
// The query `reset=1` also sets the counts to zero.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
)

type counts struct {
	mu   sync.Mutex
	data map[string]map[string]*[3]int64
}

func (c *counts) add(bucket, api string, in, out int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	apis := c.data[bucket]
	if apis == nil {
		apis = map[string]*[3]int64{}
		c.data[bucket] = apis
	}
	entry := apis[api]
	if entry == nil {
		entry = &[3]int64{}
		apis[api] = entry
	}
	entry[0]++
	entry[1] += in
	entry[2] += out
}

func (c *counts) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	body, err := json.Marshal(c.data)
	if r.URL.Query().Get("reset") == "1" {
		c.data = map[string]map[string]*[3]int64{}
	}
	c.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

// api names a request with the S3 operation names of the AWS documentation.
func api(r *http.Request, object bool) string {
	q := r.URL.Query()
	has := func(key string) bool { _, ok := q[key]; return ok }
	switch r.Method {
	case http.MethodGet:
		switch {
		case !object && has("list-type"):
			return "ListObjectsV2"
		case !object:
			return "ListObjects"
		case has("uploadId"):
			return "ListParts"
		}
		return "GetObject"
	case http.MethodHead:
		if object {
			return "HeadObject"
		}
		return "HeadBucket"
	case http.MethodPut:
		switch {
		case !object:
			return "CreateBucket"
		case has("partNumber"):
			return "UploadPart"
		case r.Header.Get("X-Amz-Copy-Source") != "":
			return "CopyObject"
		}
		return "PutObject"
	case http.MethodPost:
		switch {
		case has("uploads"):
			return "CreateMultipartUpload"
		case has("uploadId"):
			return "CompleteMultipartUpload"
		case has("delete"):
			return "DeleteObjects"
		}
	case http.MethodDelete:
		switch {
		case has("uploadId"):
			return "AbortMultipartUpload"
		case !object:
			return "DeleteBucket"
		}
		return "DeleteObject"
	}
	return r.Method
}

type countingBody struct {
	io.ReadCloser
	n int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n += int64(n)
	return n, err
}

type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *countingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9100", "address of the proxy")
	target := flag.String("target", "http://127.0.0.1:9101", "S3 service URL")
	flag.Parse()
	upstream, err := url.Parse(*target)
	if err != nil {
		log.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 4096,
		DisableCompression:  true,
	}
	c := &counts{data: map[string]map[string]*[3]int64{}}
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__counts" {
			c.serve(w, r)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		bucket, key, _ := strings.Cut(path, "/")
		name := api(r, key != "")
		var body *countingBody
		if r.Body != nil && r.Body != http.NoBody {
			body = &countingBody{ReadCloser: r.Body}
			r.Body = body
		}
		writer := &countingWriter{ResponseWriter: w}
		proxy.ServeHTTP(writer, r)
		var in int64
		if body != nil {
			in = body.n
		}
		c.add(bucket, name, in, writer.n)
	}
	log.Fatal(http.ListenAndServe(*listen, http.HandlerFunc(handler)))
}
