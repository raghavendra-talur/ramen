// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// EvLog is the per-world append-only actor event log, dumped on test failure.
type EvLog struct {
	mu sync.Mutex
	f  *os.File
}

func NewEvLog(path string) (*EvLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &EvLog{f: f}, nil
}

func (l *EvLog) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintf(l.f, "%s "+format+"\n", append([]any{time.Now().Format(time.RFC3339Nano)}, args...)...)
}

func (l *EvLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.f.Close()
}
