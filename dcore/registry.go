package dcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Registration binds one end user application (via the distributor's
// connection token) to its Web Push resources.
type Registration struct {
	// ConnectionToken identifies the app's registration with the
	// distributor (UnifiedPush spec). Every delivery carries it.
	ConnectionToken string `json:"connection_token"`
	// Application is the platform identity of the end user application:
	// the D-Bus bus name on Linux, the package name on Android. The
	// adapter uses it to address the connector.
	Application string `json:"application,omitempty"`
	// Label is a short description of the registration the distributor
	// may show to the user.
	Label string `json:"label,omitempty"`

	WebSubscription
	CreatedAt time.Time `json:"created_at"`
}

// Registry persists registrations as a single JSON file. Files keep the
// dependency surface minimal; the data is tiny and writes are rare.
type Registry struct {
	path string
	mu   sync.Mutex
	regs map[string]*Registration
}

// OpenRegistry loads (or initialises) the registry at path.
func OpenRegistry(path string) (*Registry, error) {
	r := &Registry{path: path, regs: make(map[string]*Registration)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dcore: read registry: %w", err)
	}
	if len(data) == 0 {
		return r, nil
	}
	var list []*Registration
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("dcore: parse registry: %w", err)
	}
	for _, reg := range list {
		r.regs[reg.ConnectionToken] = reg
	}
	return r, nil
}

// Get returns a registration or nil.
func (r *Registry) Get(token string) *Registration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.regs[token]
}

// List returns copies of all registrations.
func (r *Registry) List() []*Registration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Registration, 0, len(r.regs))
	for _, reg := range r.regs {
		cp := *reg
		out = append(out, &cp)
	}
	return out
}

// Put adds or replaces a registration and persists.
func (r *Registry) Put(reg *Registration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.regs[reg.ConnectionToken] = reg
	return r.saveLocked()
}

// Remove deletes a registration and persists.
func (r *Registry) Remove(token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.regs[token]; !ok {
		return nil
	}
	delete(r.regs, token)
	return r.saveLocked()
}

func (r *Registry) saveLocked() error {
	list := make([]*Registration, 0, len(r.regs))
	for _, reg := range r.regs {
		list = append(list, reg)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("dcore: write registry: %w", err)
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return fmt.Errorf("dcore: rename registry: %w", err)
	}
	return nil
}
