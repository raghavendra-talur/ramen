// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
)

//go:embed static
var staticFS embed.FS

// Server is the read-only observability endpoint. It holds no state of its
// own: everything is read from the Hub per request.
type Server struct {
	hub  *Hub
	ln   net.Listener
	http *http.Server
}

// Serve starts the UI server. addr "" binds 127.0.0.1 on an ephemeral port.
func Serve(h *Hub, addr string) (*Server, error) {
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

	s := &Server{hub: h, ln: ln}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.HandleFunc("/api/snapshot", s.snapshot)
	mux.HandleFunc("/api/stream", s.stream)
	s.http = &http.Server{Handler: mux}

	go func() { _ = s.http.Serve(ln) }()

	return s, nil
}

func (s *Server) URL() string { return "http://" + s.ln.Addr().String() }

func (s *Server) Close() { _ = s.http.Close() }

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
