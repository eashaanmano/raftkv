package shard

import (
	"fmt"
	"math"
	"testing"
)

func TestBalanceAndMinimalMovement(t *testing.T) {
	r3 := NewRing([]string{"g1", "g2", "g3"}, DefaultVNodes)
	counts := map[string]int{}
	const N = 30000
	for i := 0; i < N; i++ {
		counts[r3.Lookup(fmt.Sprintf("key%d", i))]++
	}
	for g, c := range counts {
		if dev := math.Abs(float64(c)-N/3) / (N / 3); dev > 0.2 {
			t.Errorf("group %s has %d keys (%.0f%% off even)", g, c, dev*100)
		}
	}
	// Adding a 4th group should move roughly 1/4 of keys, all of them to g4.
	r4 := NewRing([]string{"g1", "g2", "g3", "g4"}, DefaultVNodes)
	moved := 0
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key%d", i)
		a, b := r3.Lookup(k), r4.Lookup(k)
		if a != b {
			moved++
			if b != "g4" {
				t.Fatalf("key %s moved between old groups %s -> %s", k, a, b)
			}
		}
	}
	if frac := float64(moved) / N; frac < 0.15 || frac > 0.35 {
		t.Errorf("moved %.2f of keys, expected ~0.25", frac)
	}
}
