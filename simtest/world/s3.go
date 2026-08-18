// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// S3Server is an in-process S3 endpoint (path-style, no TLS) with an outage
// switch used by the fault matrix.
type S3Server struct {
	URL  string
	srv  *httptest.Server
	down atomic.Bool

	// OnChange, when set, is notified on every outage toggle.
	OnChange func(down bool)
}

func StartS3(buckets ...string) *S3Server {
	backend := s3mem.New()
	for _, b := range buckets {
		if err := backend.CreateBucket(b); err != nil {
			panic(err)
		}
	}

	s := &S3Server{}
	inner := gofakes3.New(backend).Server()
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.down.Load() {
			http.Error(w, "simtest s3 outage", http.StatusServiceUnavailable)

			return
		}
		inner.ServeHTTP(w, r)
	}))
	s.URL = s.srv.URL

	return s
}

func (s *S3Server) SetDown(down bool) {
	s.down.Store(down)
	if s.OnChange != nil {
		s.OnChange(down)
	}
}

func (s *S3Server) Stop() { s.srv.Close() }
