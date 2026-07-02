// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"net/http"
	"testing"
)

func TestS3Server(t *testing.T) {
	s := StartS3("bucket-dr1")
	t.Cleanup(s.Stop)

	resp, err := http.Get(s.URL + "/bucket-dr1?list-type=2")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("list bucket: err=%v status=%v", err, resp)
	}
	resp.Body.Close()

	s.SetDown(true)

	resp, err = http.Get(s.URL + "/bucket-dr1?list-type=2")
	if err != nil {
		t.Fatalf("get during outage: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 during outage, got %d", resp.StatusCode)
	}

	s.SetDown(false)
}
