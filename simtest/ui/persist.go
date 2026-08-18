// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"encoding/json"
	"fmt"
	"os"
)

// StartPersist tees the hub's event stream into path, one JSON line per
// event — the future replay format. Writing rides the same drop-on-full
// subscription as browsers, so persistence can never block producers.
func StartPersist(h *Hub, path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ui persist open: %w", err)
	}

	ch, cancel := h.Subscribe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		enc := json.NewEncoder(f)
		for ev := range ch {
			_ = enc.Encode(ev)
		}
	}()

	return func() {
		cancel()
		<-done
		f.Close()
	}, nil
}
