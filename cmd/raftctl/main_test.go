package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeNode(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("key") == "present" {
			_, _ = w.Write([]byte(`{"key":"present","value":"hello"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
	})
	mux.HandleFunc("PUT /v1/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"applied":true,"prev_found":true,"prev":"old"}`))
	})
	mux.HandleFunc("POST /v1/cas/{key...}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"applied":false,"prev_found":true,"prev":"actual"}`))
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":1,"role":"leader","term":3,"leader":1,"commit_index":9,"applied_index":9,"keys":2}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func exec(t *testing.T, srv *httptest.Server, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(append([]string{"--endpoints", srv.URL}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

func TestCommands(t *testing.T) {
	srv := fakeNode(t)
	cases := []struct {
		args     []string
		code     int
		out, err string
	}{
		{[]string{"get", "present"}, 0, "hello\n", ""},
		{[]string{"get", "absent"}, 1, "", "not found"},
		{[]string{"put", "k", "v"}, 0, `OK (previous value: "old")`, ""},
		{[]string{"cas", "k", "a", "b"}, 1, `not applied (current value: "actual")`, ""},
		{[]string{"status"}, 0, "leader", ""},
		{[]string{"bogus"}, 2, "", "unknown command"},
		{[]string{"put", "only-key"}, 2, "", "wrong number of arguments"},
		{nil, 2, "", "usage"},
	}
	for _, c := range cases {
		code, out, errOut := exec(t, srv, c.args...)
		if code != c.code || !strings.Contains(out, c.out) || !strings.Contains(errOut, c.err) {
			t.Errorf("%v: code=%d out=%q err=%q; want code=%d out~%q err~%q", c.args, code, out, errOut, c.code, c.out, c.err)
		}
	}
}
