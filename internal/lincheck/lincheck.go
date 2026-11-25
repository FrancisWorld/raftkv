// Package lincheck decides whether a recorded history of key-value operations
// is linearizable, using the Wing & Gong search with the memoisation refinement
// of Lowe: depth-first, try each operation that could legally go next, and
// remember (remaining operations, state) pairs already proven dead.
//
// The store is a set of independent registers, and linearizability is local, so
// the history is split per key and each key is checked on its own. That keeps
// the exponential search small.
package lincheck

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
)

// Kind is the type of an operation.
type Kind uint8

// Operation kinds.
const (
	Get Kind = iota
	Put
	Delete
	CAS
)

func (k Kind) String() string {
	return [...]string{"get", "put", "delete", "cas"}[k]
}

// Pending is the Return value of an operation whose outcome was never observed
// (the client gave up or the run ended). Such an operation may have taken effect
// at any point after its call, or never.
const Pending int64 = math.MaxInt64

// Operation is one completed or pending client operation. Call and Return are
// timestamps from any clock that orders events consistently with real time.
type Operation struct {
	Client int
	Kind   Kind
	Key    string

	Value        string // Put: value written. CAS: value to install.
	Expected     string // CAS: required current value.
	ExpectAbsent bool   // CAS: required that the key does not exist.

	Call   int64
	Return int64

	// Observed results. For Get, Found/Out are the value read. For the writers,
	// Found/Out are the previous value, and Applied says whether the write took
	// effect (always true for Put).
	Found   bool
	Out     string
	Applied bool
}

func (o Operation) String() string {
	return fmt.Sprintf("c%d %s(%q) [%d,%d] -> found=%v out=%q applied=%v", o.Client, o.Kind, o.Key, o.Call, o.Return, o.Found, o.Out, o.Applied)
}

// ErrBudgetExceeded means the search gave up before reaching a verdict. It is
// an inconclusive result, never a pass.
var ErrBudgetExceeded = errors.New("lincheck: search budget exceeded")

// Result is the verdict of Check.
type Result struct {
	Linearizable bool
	// Key is the first key found to have a non-linearizable history.
	Key string
}

// Check reports whether history is linearizable. budget bounds the number of
// search steps per key (0 selects a default of 20 million).
func Check(history []Operation, budget int) (Result, error) {
	if budget <= 0 {
		budget = 20_000_000
	}
	byKey := map[string][]Operation{}
	for _, op := range history {
		if op.Return < op.Call {
			return Result{}, fmt.Errorf("lincheck: operation returns before it is called: %v", op)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ok, err := checkKey(byKey[k], budget)
		if err != nil {
			return Result{Key: k}, fmt.Errorf("key %q: %w", k, err)
		}
		if !ok {
			return Result{Linearizable: false, Key: k}, nil
		}
	}
	return Result{Linearizable: true}, nil
}

// regState is the model state of one register.
type regState struct {
	present bool
	value   string
}

// step applies op to st. It returns the successor state and whether the
// observed output is consistent with the model. Pending operations have no
// observed output, so they are always consistent.
func step(st regState, op Operation) (regState, bool) {
	prevOK := op.Return == Pending || (op.Found == st.present && (!st.present || op.Out == st.value))
	switch op.Kind {
	case Get:
		return st, prevOK
	case Put:
		return regState{true, op.Value}, prevOK && (op.Return == Pending || op.Applied)
	case Delete:
		return regState{}, prevOK && (op.Return == Pending || op.Applied == st.present)
	case CAS:
		match := (op.ExpectAbsent && !st.present) || (!op.ExpectAbsent && st.present && st.value == op.Expected)
		next := st
		if match {
			next = regState{true, op.Value}
		}
		return next, prevOK && (op.Return == Pending || op.Applied == match)
	}
	return st, false
}

type search struct {
	ops    []Operation // sorted by Call
	dead   map[string]struct{}
	budget int
	steps  int
}

func checkKey(ops []Operation, budget int) (bool, error) {
	ops = slices.Clone(ops)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	s := &search{ops: ops, dead: map[string]struct{}{}, budget: budget}
	remaining := make([]uint64, (len(ops)+63)/64)
	for i := range ops {
		remaining[i/64] |= 1 << (i % 64)
	}
	ok := s.dfs(remaining, regState{})
	if s.steps > s.budget {
		return false, ErrBudgetExceeded
	}
	return ok, nil
}

func (s *search) memoKey(remaining []uint64, st regState) string {
	var b strings.Builder
	for _, w := range remaining {
		fmt.Fprintf(&b, "%x.", w)
	}
	if st.present {
		b.WriteByte('+')
		b.WriteString(st.value)
	}
	return b.String()
}

func (s *search) dfs(remaining []uint64, st regState) bool {
	if s.steps++; s.steps > s.budget {
		return false
	}
	// Smallest return time among operations still to be placed: anything called
	// after it cannot precede it in real time.
	minReturn, completed := Pending, false
	for i := range s.ops {
		if remaining[i/64]&(1<<(i%64)) != 0 && s.ops[i].Return != Pending {
			completed = true
			minReturn = min(minReturn, s.ops[i].Return)
		}
	}
	if !completed {
		return true // leftover pending operations simply never took effect
	}
	key := s.memoKey(remaining, st)
	if _, dead := s.dead[key]; dead {
		return false
	}
	for i := range s.ops {
		if remaining[i/64]&(1<<(i%64)) == 0 {
			continue
		}
		op := s.ops[i]
		if op.Call > minReturn {
			break // ops are sorted by call time; the rest are later still
		}
		next, ok := step(st, op)
		if !ok {
			continue
		}
		remaining[i/64] &^= 1 << (i % 64)
		found := s.dfs(remaining, next)
		remaining[i/64] |= 1 << (i % 64)
		if found {
			return true
		}
		if s.steps > s.budget {
			return false
		}
	}
	s.dead[key] = struct{}{}
	return false
}
