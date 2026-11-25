package node

import (
	"context"
	"sync"
)

// Future is the eventual outcome of a proposal or read barrier. It is
// resolved exactly once, from inside the node's apply loop.
type Future struct {
	once sync.Once
	done chan struct{}
	val  []byte
	err  error
}

func newFuture() *Future { return &Future{done: make(chan struct{})} }

func failedFuture(err error) *Future {
	f := newFuture()
	f.resolve(nil, err)
	return f
}

func (f *Future) resolve(val []byte, err error) {
	f.once.Do(func() {
		f.val, f.err = val, err
		close(f.done)
	})
}

// Done is closed once the outcome is known.
func (f *Future) Done() <-chan struct{} { return f.done }

// Result returns the outcome. It must only be called after Done is closed.
func (f *Future) Result() ([]byte, error) { return f.val, f.err }

// Wait blocks until the outcome is known or ctx ends. A context error says
// nothing about whether the operation eventually took effect.
func (f *Future) Wait(ctx context.Context) ([]byte, error) {
	select {
	case <-f.done:
		return f.val, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
