// Package filestore persists dcore registrations in a JSON file.
//
// The dcore library keeps registrations in memory and reports every change
// through Options.OnChanged; storing them is the embedding application's
// job. This package is the smallest thing that works: apply each change to
// an in-memory map, rewrite the file atomically, and hand the saved records
// back with Distributor.Restore at startup. A production application would
// swap the rewrite-per-change save for an append-only log or a key-value
// store without touching the library.
package filestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

// Store is the change sink for a dcore.Distributor.
type Store struct {
	path string

	mu   sync.Mutex
	regs map[string]dcore.Registration
}

// Open loads the registrations saved at path. A missing file is an empty
// store, not an error.
func Open(path string) (*Store, error) {
	s := &Store{path: path, regs: make(map[string]dcore.Registration)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) || (err == nil && len(data) == 0) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: read %s: %w", path, err)
	}
	var list []dcore.Registration
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("filestore: parse %s: %w", path, err)
	}
	for _, reg := range list {
		s.regs[reg.ConnectionToken] = reg
	}
	return s, nil
}

// Registrations returns the saved records, for Distributor.Restore.
func (s *Store) Registrations() []dcore.Registration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Collect(maps.Values(s.regs))
}

// Apply is the dcore.Options.OnChanged hook.
func (s *Store) Apply(kind dcore.ChangeKind, reg dcore.Registration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kind == dcore.Removed {
		// Removing a record that is not there is not an error.
		delete(s.regs, reg.ConnectionToken)
	} else {
		s.regs[reg.ConnectionToken] = reg
	}
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	list := make([]dcore.Registration, 0, len(s.regs))
	for _, reg := range s.regs {
		list = append(list, reg)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("filestore: write %s: %w", s.path, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("filestore: rename %s: %w", s.path, err)
	}
	return nil
}
