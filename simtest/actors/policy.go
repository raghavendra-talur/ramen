// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"strings"
	"sync"
	"time"
)

// Key identifies one actor instance on one cluster; policies are set per key.
type Key struct{ Actor, Cluster string }

func (k Key) String() string { return k.Actor + "@" + k.Cluster }

func VolRep(cluster string) Key { return Key{Actor: "volrep", Cluster: cluster} }
func Work(cluster string) Key   { return Key{Actor: "work", Cluster: cluster} }
func View(cluster string) Key   { return Key{Actor: "view", Cluster: cluster} }
func Binder(cluster string) Key { return Key{Actor: "binder", Cluster: cluster} }

type Policy interface{ isPolicy() }

// Normal fulfills immediately.
type Normal struct{}

// Silent never responds while set (external system down); actors poll so
// clearing the policy resumes fulfillment.
type Silent struct{}

// Delayed responds only after the object has been pending for After.
type Delayed struct{ After time.Duration }

// FailWith fulfills with an actor-specific failure payload. Modes are defined
// by each actor (volrep: "validated-false", "degraded").
type FailWith struct{ Mode string }

func (Normal) isPolicy()   {}
func (Silent) isPolicy()   {}
func (Delayed) isPolicy()  {}
func (FailWith) isPolicy() {}

const pollInterval = 300 * time.Millisecond

type Decision struct {
	Proceed      bool
	RequeueAfter time.Duration
	Policy       Policy
}

type Store struct {
	mu        sync.Mutex
	policies  map[Key]Policy
	firstSeen map[string]time.Time // key.String()+"/"+obj -> first Decide under Delayed

	// OnChange, when set, is notified after every Set (the UI hub tee).
	OnChange func(Key, Policy)
}

func NewStore() *Store {
	return &Store{policies: map[Key]Policy{}, firstSeen: map[string]time.Time{}}
}

func (s *Store) Set(k Key, p Policy) {
	s.mu.Lock()

	s.policies[k] = p

	if _, ok := p.(Delayed); !ok { // reset delay clocks for this key only
		keyPrefix := k.String() + "/"
		for id := range s.firstSeen {
			if strings.HasPrefix(id, keyPrefix) {
				delete(s.firstSeen, id)
			}
		}
	}

	s.mu.Unlock()

	if s.OnChange != nil {
		s.OnChange(k, p)
	}
}

func (s *Store) Decide(k Key, obj string) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.policies[k]
	if !ok {
		p = Normal{}
	}

	switch pol := p.(type) {
	case Silent:
		return Decision{Proceed: false, RequeueAfter: pollInterval, Policy: pol}
	case Delayed:
		id := k.String() + "/" + obj
		t0, seen := s.firstSeen[id]
		if !seen {
			t0 = time.Now()
			s.firstSeen[id] = t0
		}
		if time.Since(t0) < pol.After {
			return Decision{Proceed: false, RequeueAfter: pollInterval, Policy: pol}
		}

		// cleanup: deadline passed, remove from tracking to prevent unbounded growth
		delete(s.firstSeen, id)
		return Decision{Proceed: true, Policy: pol}
	default:
		return Decision{Proceed: true, Policy: p}
	}
}
