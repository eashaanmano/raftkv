// Package shard maps keys to replica groups with consistent hashing.
//
// Each group owns VNodes points on a 64-bit hash ring; a key belongs to the
// first point clockwise from hash(key). Virtual nodes even out the load, and
// adding or removing a group only moves ~1/N of the keys — the property that
// makes consistent hashing worth it over hash(key) % N.
package shard

import (
	"hash/fnv"
	"sort"
	"strconv"
)

const DefaultVNodes = 128

type point struct {
	hash  uint64
	group string
}

// Ring is immutable once built and safe for concurrent use.
type Ring struct {
	points []point
	groups []string
}

func NewRing(groups []string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	r := &Ring{groups: append([]string(nil), groups...)}
	for _, g := range groups {
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, point{hash: Hash(g + "#" + strconv.Itoa(i)), group: g})
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
	return r
}

// Hash is FNV-1a followed by a splitmix64 finalizer (FNV alone has weak
// avalanche on short, similar keys like "user1", "user2").
func Hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Lookup returns the group that owns key.
func (r *Ring) Lookup(key string) string {
	if len(r.points) == 0 {
		return ""
	}
	h := Hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrap around
	}
	return r.points[i].group
}

func (r *Ring) Groups() []string { return append([]string(nil), r.groups...) }
