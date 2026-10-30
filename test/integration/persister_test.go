package integration

import (
	"errors"
	"sync"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

// persister is the embedding application's half of dcore's storage
// contract: it receives the change events and can hand the saved records
// back to a fresh distributor. The library itself stores nothing.
type persister struct {
	mu   sync.Mutex
	regs map[string]dcore.Registration

	// failNext makes that many writes fail: the distributor must then
	// refuse to hand out an endpoint it cannot remember.
	failNext int
	fail     error
	Writes   int
}

func newPersister() *persister {
	return &persister{regs: map[string]dcore.Registration{}}
}

// apply is the dcore.Options.OnChanged hook.
func (p *persister) apply(kind dcore.ChangeKind, reg dcore.Registration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Writes++
	if p.failNext > 0 {
		p.failNext--
		return p.fail
	}
	if kind == dcore.Removed {
		delete(p.regs, reg.ConnectionToken)
		return nil
	}
	p.regs[reg.ConnectionToken] = reg
	return nil
}

func (p *persister) all() []dcore.Registration {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]dcore.Registration, 0, len(p.regs))
	for _, reg := range p.regs {
		out = append(out, reg)
	}
	return out
}

func (p *persister) has(token string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.regs[token]
	return ok
}

func (p *persister) failNextWrites(n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext, p.fail = n, err
}

var errDiskFull = errors.New("integration: storage is full")
