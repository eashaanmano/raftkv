// Package lincheck decides whether a concurrent history of key-value
// operations is linearizable, using the Wing–Gong–Lowe algorithm (a
// backtracking search with memoisation of (linearized-set, state) pairs, as in
// Lowe's "Testing for linearizability" and the Porcupine checker).
//
// Histories are partitioned by key first: a KV history is linearizable iff
// each per-key sub-history is (linearizability is local/compositional), which
// keeps the search small.
package lincheck

import (
	"fmt"
	"sort"
	"time"
)

type Kind uint8

const (
	Get Kind = iota
	Put
	Append
)

func (k Kind) String() string { return [...]string{"get", "put", "append"}[k] }

// Op is one completed (or timed-out) client operation.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	Value  string // input for put/append, observed output for get
	Call   int64  // logical timestamp when the request was issued
	Return int64  // logical timestamp when the response arrived
	// Unknown marks a write whose response never arrived: it may or may not
	// have taken effect, at any time after Call. Modelled as Return = +inf.
	Unknown bool
}

func (o Op) String() string {
	ret := fmt.Sprint(o.Return)
	if o.Unknown {
		ret = "?"
	}
	return fmt.Sprintf("c%d %s(%q,%q) [%d,%s]", o.Client, o.Kind, o.Key, o.Value, o.Call, ret)
}

type Result struct {
	OK       bool
	TimedOut bool // search budget exhausted; result is inconclusive
	BadKey   string
	Ops      int
	Keys     int
}

// CheckKV checks a history. timeout bounds the total search time.
func CheckKV(history []Op, timeout time.Duration) Result {
	byKey := map[string][]Op{}
	for _, o := range history {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	deadline := time.Now().Add(timeout)
	res := Result{OK: true, Ops: len(history), Keys: len(keys)}
	for _, k := range keys {
		ok, timedOut := checkKey(byKey[k], deadline)
		if timedOut {
			res.TimedOut = true
			continue
		}
		if !ok {
			res.OK = false
			res.BadKey = k
			return res
		}
	}
	return res
}

type entry struct {
	op         int
	isCall     bool
	time       int64
	match      *entry // call <-> return
	prev, next *entry
}

const inf = int64(1) << 62

func checkKey(ops []Op, deadline time.Time) (ok, timedOut bool) {
	// Build the event list: a call and a return event per op, ordered by time.
	events := make([]*entry, 0, 2*len(ops))
	for i, o := range ops {
		ret := o.Return
		if o.Unknown {
			ret = inf
		}
		c := &entry{op: i, isCall: true, time: o.Call}
		r := &entry{op: i, isCall: false, time: ret}
		c.match, r.match = r, c
		events = append(events, c, r)
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].time != events[j].time {
			return events[i].time < events[j].time
		}
		// At equal times, put returns first: ops that merely touch are not concurrent.
		return !events[i].isCall && events[j].isCall
	})
	head := &entry{}
	prev := head
	for _, e := range events {
		prev.next, e.prev = e, prev
		prev = e
	}

	type frame struct {
		e     *entry
		state string
	}
	words := (len(ops) + 63) / 64
	linearized := make([]uint64, words)
	cache := map[string]struct{}{}
	var stack []frame
	state := ""
	e := head.next
	steps := 0
	for head.next != nil {
		if steps++; steps&0xfff == 0 && time.Now().After(deadline) {
			return false, true
		}
		if e.isCall {
			o := ops[e.op]
			newState, legal := stepKV(state, o)
			if legal {
				linearized[e.op/64] |= 1 << (e.op % 64)
				key := cacheKey(linearized, newState)
				if _, seen := cache[key]; !seen {
					cache[key] = struct{}{}
					stack = append(stack, frame{e, state})
					state = newState
					lift(e)
					e = head.next
					continue
				}
				linearized[e.op/64] &^= 1 << (e.op % 64)
			}
			e = e.next
		} else {
			// Reached a return whose op is not yet linearized: backtrack.
			if len(stack) == 0 {
				return false, false
			}
			f := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			state = f.state
			linearized[f.e.op/64] &^= 1 << (f.e.op % 64)
			unlift(f.e)
			e = f.e.next
		}
	}
	return true, false
}

func stepKV(state string, o Op) (string, bool) {
	switch o.Kind {
	case Put:
		return o.Value, true
	case Append:
		return state + o.Value, true
	default: // Get
		return state, o.Value == state
	}
}

func lift(e *entry) {
	e.prev.next = e.next
	if e.next != nil {
		e.next.prev = e.prev
	}
	m := e.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

func unlift(e *entry) {
	m := e.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	e.prev.next = e
	if e.next != nil {
		e.next.prev = e
	}
}

func cacheKey(bits []uint64, state string) string {
	b := make([]byte, 0, len(bits)*8+len(state)+1)
	for _, w := range bits {
		b = append(b, byte(w), byte(w>>8), byte(w>>16), byte(w>>24), byte(w>>32), byte(w>>40), byte(w>>48), byte(w>>56))
	}
	b = append(b, 0)
	return string(append(b, state...))
}
