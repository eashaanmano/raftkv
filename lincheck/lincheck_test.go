package lincheck

import (
	"testing"
	"time"
)

func check(t *testing.T, h []Op) bool {
	t.Helper()
	r := CheckKV(h, 5*time.Second)
	if r.TimedOut {
		t.Fatal("checker timed out")
	}
	return r.OK
}

func TestSequentialOK(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", Value: "1", Call: 2, Return: 3},
		{Kind: Append, Key: "x", Value: "2", Call: 4, Return: 5},
		{Kind: Get, Key: "x", Value: "12", Call: 6, Return: 7},
	}
	if !check(t, h) {
		t.Fatal("valid sequential history rejected")
	}
}

func TestStaleReadRejected(t *testing.T) {
	// The put completes before the get starts, yet the get misses it.
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", Value: "", Call: 2, Return: 3},
	}
	if check(t, h) {
		t.Fatal("stale read accepted")
	}
}

func TestConcurrentEitherOrder(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10},
		{Client: 2, Kind: Put, Key: "x", Value: "b", Call: 1, Return: 9},
		{Client: 3, Kind: Get, Key: "x", Value: "a", Call: 11, Return: 12},
	}
	if !check(t, h) {
		t.Fatal("valid concurrent history rejected")
	}
}

func TestNonMonotonicReadsRejected(t *testing.T) {
	// Two sequential reads by one client see b, then a: impossible after both writes finished.
	h := []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 3},
		{Client: 2, Kind: Put, Key: "x", Value: "b", Call: 1, Return: 2},
		{Client: 3, Kind: Get, Key: "x", Value: "b", Call: 4, Return: 5},
		{Client: 3, Kind: Get, Key: "x", Value: "a", Call: 6, Return: 7},
	}
	if check(t, h) {
		t.Fatal("non-linearizable history accepted")
	}
}

func TestUnknownWriteMayOrMayNotHappen(t *testing.T) {
	never := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Unknown: true},
		{Kind: Get, Key: "x", Value: "", Call: 5, Return: 6},
	}
	happened := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Unknown: true},
		{Kind: Get, Key: "x", Value: "1", Call: 5, Return: 6},
	}
	if !check(t, never) || !check(t, happened) {
		t.Fatal("timed-out write must be allowed to have happened or not")
	}
	flipFlop := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Unknown: true},
		{Kind: Get, Key: "x", Value: "1", Call: 5, Return: 6},
		{Kind: Get, Key: "x", Value: "", Call: 7, Return: 8},
	}
	if check(t, flipFlop) {
		t.Fatal("a write cannot be un-applied")
	}
}
