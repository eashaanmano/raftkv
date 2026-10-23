// Package clusterconf loads the JSON file that describes a deployment:
//
//	{
//	  "groups": {
//	    "g1": [{"id": "n1", "addr": "n1:7001"}, {"id": "n2", "addr": "n2:7001"}, ...],
//	    "g2": [...]
//	  }
//	}
//
// Keys are sharded across groups with the consistent-hash ring; the file is
// the initial configuration (later membership changes live in each group's log).
package clusterconf

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"raftkv/raft"
	"raftkv/shard"
)

type File struct {
	Groups map[string][]raft.Member `json:"groups"`
	VNodes int                      `json:"vnodes,omitempty"`
}

func Load(path string) (*File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(f.Groups) == 0 {
		return nil, fmt.Errorf("%s: no groups", path)
	}
	return &f, nil
}

func (f *File) GroupNames() []string {
	var gs []string
	for g := range f.Groups {
		gs = append(gs, g)
	}
	sort.Strings(gs)
	return gs
}

func (f *File) Ring() *shard.Ring { return shard.NewRing(f.GroupNames(), f.VNodes) }

// Find returns the group and member entry for a server ID.
func (f *File) Find(id string) (string, raft.Member, bool) {
	for g, ms := range f.Groups {
		for _, m := range ms {
			if m.ID == id {
				return g, m, true
			}
		}
	}
	return "", raft.Member{}, false
}
