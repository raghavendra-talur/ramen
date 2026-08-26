// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

//go:embed static
var staticFS embed.FS

// Server is the read-only observability endpoint. It holds no state of its
// own: everything is read from the Hub (or the run's log files) per request.
type Server struct {
	hub    *Hub
	ln     net.Listener
	http   *http.Server
	logDir string
}

// Serve starts the UI server. addr "" binds 127.0.0.1 on an ephemeral port;
// logDir is the artifacts dir holding the manager/actor logs ("" disables
// the logs endpoint).
func Serve(h *Hub, addr, logDir string) (*Server, error) {
	if addr == "" || addr == "1" { // SIMTEST_UI=1 means "on, pick a port"
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ui listen %s: %w", addr, err)
	}

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("ui static fs: %w", err)
	}

	s := &Server{hub: h, ln: ln, logDir: logDir}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.HandleFunc("/api/snapshot", s.snapshot)
	mux.HandleFunc("/api/stream", s.stream)
	mux.HandleFunc("/api/logs", s.logs)
	s.http = &http.Server{Handler: mux}

	go func() { _ = s.http.Serve(ln) }()

	return s, nil
}

func (s *Server) URL() string { return "http://" + s.ln.Addr().String() }

func (s *Server) Close() { _ = s.http.Close() }

// logSources whitelists what /api/logs may read: the three ramen manager
// logs and the actors log, never an arbitrary path.
var logSources = map[string]bool{"hub": true, "dr1": true, "dr2": true, "actors": true}

const (
	logTailDefault = 400
	logTailMax     = 2000
	logReadBackCap = 1 << 20 // read at most the final 1MiB of a log
)

// logs serves the tail of one run log as text/plain.
func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	src := r.URL.Query().Get("src")
	if !logSources[src] || s.logDir == "" {
		http.Error(w, "unknown log source", http.StatusBadRequest)

		return
	}

	tail := logTailDefault
	if n, err := strconv.Atoi(r.URL.Query().Get("tail")); err == nil && n > 0 && n <= logTailMax {
		tail = n
	}

	lines, err := tailFile(filepath.Join(s.logDir, src+".log"), tail)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

// tailFile returns the last n lines of path, reading at most the final
// logReadBackCap bytes.
func tailFile(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}

	off := int64(0)
	if st.Size() > logReadBackCap {
		off = st.Size() - logReadBackCap
	}

	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil && err != io.EOF {
		return nil, err
	}

	lines := strings.Split(strings.TrimRight(string(buf), "\n"), "\n")
	if off > 0 && len(lines) > 0 {
		lines = lines[1:] // first line may be cut mid-way by the cap
	}

	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	return lines, nil
}

func (s *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.hub.Snapshot())
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fl.Flush()

	ch, cancel := s.hub.Subscribe()
	defer cancel()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			// Channel is closed when hub.Subscribe's cancel is called,
			// allowing this goroutine to terminate cleanly.
			if !ok {
				return
			}
			b, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			fl.Flush()
		}
	}
}
