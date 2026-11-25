package lincheck

import (
	"errors"
	"math/rand/v2"
	"testing"
)

func put(c int, key, val string, call, ret int64, prevFound bool, prev string) Operation {
	return Operation{Client: c, Kind: Put, Key: key, Value: val, Call: call, Return: ret, Found: prevFound, Out: prev, Applied: true}
}

func get(c int, key string, call, ret int64, found bool, val string) Operation {
	return Operation{Client: c, Kind: Get, Key: key, Call: call, Return: ret, Found: found, Out: val}
}

func mustCheck(t *testing.T, h []Operation) bool {
	t.Helper()
	r, err := Check(h, 0)
	if err != nil {
		t.Fatal(err)
	}
	return r.Linearizable
}

func TestSequentialHistory(t *testing.T) {
	h := []Operation{
		get(1, "k", 1, 2, false, ""),
		put(1, "k", "a", 3, 4, false, ""),
		get(1, "k", 5, 6, true, "a"),
		put(1, "k", "b", 7, 8, true, "a"),
		get(1, "k", 9, 10, true, "b"),
	}
	if !mustCheck(t, h) {
		t.Fatal("sequential history rejected")
	}
}

func TestConcurrentReadMaySeeEitherSide(t *testing.T) {
	for _, seen := range []struct {
		found bool
		val   string
	}{{false, ""}, {true, "a"}} {
		h := []Operation{
			put(1, "k", "a", 1, 10, false, ""),
			get(2, "k", 2, 9, seen.found, seen.val),
		}
		if !mustCheck(t, h) {
			t.Fatalf("overlapping read of %+v rejected", seen)
		}
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	h := []Operation{
		put(1, "k", "a", 1, 2, false, ""),
		get(2, "k", 3, 4, false, ""), // starts after the put returned yet misses it
	}
	if mustCheck(t, h) {
		t.Fatal("stale read accepted")
	}
}

func TestReadsCannotGoBackwardsInTime(t *testing.T) {
	h := []Operation{
		put(1, "k", "a", 1, 20, false, ""),
		get(2, "k", 2, 3, true, "a"),
		get(3, "k", 5, 6, false, ""), // a later read un-sees the write
	}
	if mustCheck(t, h) {
		t.Fatal("non-monotonic reads accepted")
	}
}

func TestLostUpdateIsRejected(t *testing.T) {
	// Two sequential puts both claim the key was absent before them.
	h := []Operation{
		put(1, "k", "a", 1, 2, false, ""),
		put(2, "k", "b", 3, 4, false, ""),
	}
	if mustCheck(t, h) {
		t.Fatal("lost update accepted")
	}
}

func TestCAS(t *testing.T) {
	cas := func(c int, call, ret int64, exp string, absent bool, val string, applied, found bool, prev string) Operation {
		return Operation{Client: c, Kind: CAS, Key: "k", Expected: exp, ExpectAbsent: absent, Value: val,
			Call: call, Return: ret, Applied: applied, Found: found, Out: prev}
	}
	good := []Operation{
		cas(1, 1, 2, "", true, "a", true, false, ""),
		cas(1, 3, 4, "a", false, "b", true, true, "a"),
		cas(2, 5, 6, "a", false, "c", false, true, "b"),
		get(2, "k", 7, 8, true, "b"),
	}
	if !mustCheck(t, good) {
		t.Fatal("valid CAS history rejected")
	}
	// Two concurrent CAS(a->b) and CAS(a->c) cannot both succeed.
	bad := []Operation{
		put(1, "k", "a", 1, 2, false, ""),
		cas(2, 3, 10, "a", false, "b", true, true, "a"),
		cas(3, 4, 9, "a", false, "c", true, true, "a"),
	}
	if mustCheck(t, bad) {
		t.Fatal("double-successful CAS accepted")
	}
}

func TestDelete(t *testing.T) {
	del := func(call, ret int64, applied, found bool, prev string) Operation {
		return Operation{Client: 1, Kind: Delete, Key: "k", Call: call, Return: ret, Applied: applied, Found: found, Out: prev}
	}
	h := []Operation{
		put(1, "k", "a", 1, 2, false, ""),
		del(3, 4, true, true, "a"),
		del(5, 6, false, false, ""),
		get(1, "k", 7, 8, false, ""),
	}
	if !mustCheck(t, h) {
		t.Fatal("valid delete history rejected")
	}
	h[1].Applied = false // claims the delete found nothing although the key existed
	if mustCheck(t, h) {
		t.Fatal("inconsistent delete accepted")
	}
}

func TestPendingOperationMayOrMayNotHaveTakenEffect(t *testing.T) {
	pending := Operation{Client: 1, Kind: Put, Key: "k", Value: "a", Call: 1, Return: Pending}
	for _, seen := range []struct {
		found bool
		val   string
	}{{false, ""}, {true, "a"}} {
		h := []Operation{pending, get(2, "k", 5, 6, seen.found, seen.val)}
		if !mustCheck(t, h) {
			t.Fatalf("pending put with read %+v rejected", seen)
		}
	}
	// ...but it cannot explain a value nobody wrote.
	if mustCheck(t, []Operation{pending, get(2, "k", 5, 6, true, "zzz")}) {
		t.Fatal("read of an unwritten value accepted")
	}
	// ...and it cannot take effect before it was called.
	early := []Operation{get(2, "k", 1, 2, true, "a"), {Client: 1, Kind: Put, Key: "k", Value: "a", Call: 3, Return: Pending}}
	if mustCheck(t, early) {
		t.Fatal("effect observed before the pending call accepted")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Operation{
		put(1, "x", "1", 1, 2, false, ""),
		put(1, "y", "2", 3, 4, false, ""),
		get(2, "x", 5, 6, true, "1"),
		get(2, "y", 5, 6, true, "2"),
	}
	if !mustCheck(t, h) {
		t.Fatal("independent keys rejected")
	}
	h = append(h, get(3, "y", 7, 8, false, ""))
	r, err := Check(h, 0)
	if err != nil || r.Linearizable || r.Key != "y" {
		t.Fatalf("result = %+v err = %v, want violation on y", r, err)
	}
}

func TestRejectsMalformedOperation(t *testing.T) {
	if _, err := Check([]Operation{get(1, "k", 5, 2, false, "")}, 0); err == nil {
		t.Fatal("expected an error for return < call")
	}
}

func TestBudgetExceededIsInconclusiveNotAPass(t *testing.T) {
	h := []Operation{
		put(1, "k", "a", 1, 2, false, ""),
		put(1, "k", "b", 3, 4, true, "a"),
		put(1, "k", "c", 5, 6, true, "b"),
		get(1, "k", 7, 8, true, "c"),
	}
	if _, err := Check(h, 2); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("err = %v, want ErrBudgetExceeded", err)
	}
}

// Property: a history generated from a legal serial execution, with each
// operation's interval widened randomly around its linearization point, must
// always be accepted.
func TestGeneratedLinearizableHistoriesAreAccepted(t *testing.T) {
	for seed := uint64(0); seed < 60; seed++ {
		rng := rand.New(rand.NewPCG(seed, 1))
		var st regState
		var h []Operation
		for i := 0; i < 40; i++ {
			at := int64(i)*10 + 5
			op := Operation{
				Client: rng.IntN(5), Key: "k",
				Call: at - 1 - rng.Int64N(25), Return: at + 1 + rng.Int64N(25),
			}
			val := string(rune('a' + rng.IntN(4)))
			switch rng.IntN(3) {
			case 0:
				op.Kind = Get
				op.Found, op.Out = st.present, st.value
			case 1:
				op.Kind, op.Value, op.Applied = Put, val, true
				op.Found, op.Out = st.present, st.value
				st = regState{true, val}
			default:
				op.Kind, op.Expected, op.Value = CAS, string(rune('a'+rng.IntN(4))), val
				match := st.present && st.value == op.Expected
				op.Applied, op.Found, op.Out = match, st.present, st.value
				if match {
					st = regState{true, val}
				}
			}
			h = append(h, op)
		}
		r, err := Check(h, 0)
		if err != nil || !r.Linearizable {
			t.Fatalf("seed %d: generated linearizable history rejected (err=%v)", seed, err)
		}
	}
}
