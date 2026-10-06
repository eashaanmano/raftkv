package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists Raft's hard state, log and snapshot. Every call must be
// durable when it returns (Raft's safety argument assumes it).
type Storage interface {
	// Load returns everything persisted. snap is nil if no snapshot exists.
	// entries contains only entries after the snapshot index.
	Load() (hs HardState, snap *Snapshot, entries []Entry, err error)
	// Save atomically (one fsync) records: a new hard state (if hs != nil),
	// deletion of all entries with index >= truncateFrom (if truncateFrom > 0),
	// then appends entries.
	Save(hs *HardState, truncateFrom uint64, entries []Entry) error
	// SaveSnapshot installs snap and replaces the log with remaining.
	SaveSnapshot(hs HardState, snap *Snapshot, remaining []Entry) error
	Close() error
}

// ---------------------------------------------------------------------------
// MemStorage: used by tests. A "crashed" node is restarted with the same
// MemStorage object, which models a disk that survived the crash.
// ---------------------------------------------------------------------------

type MemStorage struct {
	mu      sync.Mutex
	hs      HardState
	snap    *Snapshot
	entries []Entry
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Load() (HardState, *Snapshot, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s *Snapshot
	if m.snap != nil {
		c := *m.snap
		s = &c
	}
	return m.hs, s, append([]Entry(nil), m.entries...), nil
}

func (m *MemStorage) Save(hs *HardState, truncateFrom uint64, entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hs != nil {
		m.hs = *hs
	}
	if truncateFrom > 0 {
		m.entries = truncateEntries(m.entries, truncateFrom)
	}
	m.entries = appendEntries(m.entries, entries)
	return nil
}

func (m *MemStorage) SaveSnapshot(hs HardState, snap *Snapshot, remaining []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := *snap
	m.hs = hs
	m.snap = &c
	m.entries = append([]Entry(nil), remaining...)
	return nil
}

func (m *MemStorage) Close() error { return nil }

func truncateEntries(es []Entry, from uint64) []Entry {
	for i, e := range es {
		if e.Index >= from {
			return es[:i]
		}
	}
	return es
}

// appendEntries appends, overwriting any existing suffix that overlaps.
func appendEntries(es []Entry, add []Entry) []Entry {
	if len(add) == 0 {
		return es
	}
	es = truncateEntries(es, add[0].Index)
	return append(es, add...)
}

// ---------------------------------------------------------------------------
// FileStorage: a write-ahead log plus a snapshot file.
//
// WAL record framing: [u32 payload length][u32 crc32(payload)][payload]
// payload: [u8 type][body]
//   type 1 hardstate: u64 term, u32 len, votedFor
//   type 2 truncate:  u64 fromIndex
//   type 3 entries:   u32 count, then per entry: u64 index, u64 term, u8 type, u32 len, data
// A torn or corrupt tail record (crash mid-write) is detected by length/CRC
// and discarded on load; everything before it was fsynced and is kept.
// ---------------------------------------------------------------------------

const (
	recHardState = 1
	recTruncate  = 2
	recEntries   = 3
)

type FileStorage struct {
	mu  sync.Mutex
	dir string
	wal *os.File
	buf []byte
}

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{dir: dir}
	return fs, nil
}

func (f *FileStorage) walPath() string  { return filepath.Join(f.dir, "wal") }
func (f *FileStorage) snapPath() string { return filepath.Join(f.dir, "snapshot") }

func (f *FileStorage) Load() (HardState, *Snapshot, []Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var hs HardState
	snap, err := readSnapshotFile(f.snapPath())
	if err != nil {
		return hs, nil, nil, err
	}
	var entries []Entry
	wal, err := os.OpenFile(f.walPath(), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return hs, nil, nil, err
	}
	r := bufio.NewReader(wal)
	var good int64
	for {
		payload, n, err := readRecord(r)
		if err != nil {
			break // EOF or torn tail
		}
		if len(payload) == 0 {
			break
		}
		switch payload[0] {
		case recHardState:
			hs, err = decodeHardState(payload[1:])
		case recTruncate:
			if len(payload) < 9 {
				err = errors.New("short truncate record")
				break
			}
			entries = truncateEntries(entries, binary.LittleEndian.Uint64(payload[1:]))
		case recEntries:
			var es []Entry
			es, err = decodeEntries(payload[1:])
			entries = appendEntries(entries, es)
		default:
			err = fmt.Errorf("unknown record type %d", payload[0])
		}
		if err != nil {
			break
		}
		good += int64(n)
	}
	// Drop a torn tail so future appends start at a clean boundary.
	if err := wal.Truncate(good); err != nil {
		wal.Close()
		return hs, nil, nil, err
	}
	if _, err := wal.Seek(good, io.SeekStart); err != nil {
		wal.Close()
		return hs, nil, nil, err
	}
	f.wal = wal
	if snap != nil {
		// Entries covered by the snapshot may survive if we crashed between
		// writing the snapshot and rewriting the WAL.
		i := 0
		for i < len(entries) && entries[i].Index <= snap.Index {
			i++
		}
		entries = entries[i:]
	}
	return hs, snap, entries, nil
}

func (f *FileStorage) Save(hs *HardState, truncateFrom uint64, entries []Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return errors.New("storage: Load must be called before Save")
	}
	f.buf = f.buf[:0]
	if hs != nil {
		f.buf = appendRecord(f.buf, encodeHardState(*hs))
	}
	if truncateFrom > 0 {
		p := make([]byte, 9)
		p[0] = recTruncate
		binary.LittleEndian.PutUint64(p[1:], truncateFrom)
		f.buf = appendRecord(f.buf, p)
	}
	if len(entries) > 0 {
		f.buf = appendRecord(f.buf, encodeEntries(entries))
	}
	if len(f.buf) == 0 {
		return nil
	}
	if _, err := f.wal.Write(f.buf); err != nil {
		return err
	}
	return f.wal.Sync()
}

func (f *FileStorage) SaveSnapshot(hs HardState, snap *Snapshot, remaining []Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// 1. snapshot file, atomically.
	if err := writeFileAtomic(f.snapPath(), encodeSnapshot(snap)); err != nil {
		return err
	}
	// 2. rewrite the WAL with only the surviving entries (log compaction).
	var buf []byte
	buf = appendRecord(buf, encodeHardState(hs))
	if len(remaining) > 0 {
		buf = appendRecord(buf, encodeEntries(remaining))
	}
	if f.wal != nil {
		f.wal.Close()
	}
	if err := writeFileAtomic(f.walPath(), buf); err != nil {
		return err
	}
	wal, err := os.OpenFile(f.walPath(), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	f.wal = wal
	return nil
}

func (f *FileStorage) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal != nil {
		err := f.wal.Close()
		f.wal = nil
		return err
	}
	return nil
}

// ---- encoding helpers ----

func appendRecord(buf, payload []byte) []byte {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	buf = append(buf, hdr[:]...)
	return append(buf, payload...)
}

func readRecord(r *bufio.Reader) ([]byte, int, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:])
	if n > 1<<30 {
		return nil, 0, errors.New("record too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:]) {
		return nil, 0, errors.New("crc mismatch")
	}
	return payload, int(8 + n), nil
}

func encodeHardState(hs HardState) []byte {
	p := make([]byte, 1+8+4+len(hs.VotedFor))
	p[0] = recHardState
	binary.LittleEndian.PutUint64(p[1:], hs.Term)
	binary.LittleEndian.PutUint32(p[9:], uint32(len(hs.VotedFor)))
	copy(p[13:], hs.VotedFor)
	return p
}

func decodeHardState(b []byte) (HardState, error) {
	if len(b) < 12 {
		return HardState{}, errors.New("short hardstate")
	}
	n := binary.LittleEndian.Uint32(b[8:])
	if len(b) < 12+int(n) {
		return HardState{}, errors.New("short hardstate")
	}
	return HardState{Term: binary.LittleEndian.Uint64(b), VotedFor: string(b[12 : 12+n])}, nil
}

func encodeEntries(es []Entry) []byte {
	size := 1 + 4
	for _, e := range es {
		size += 8 + 8 + 1 + 4 + len(e.Data)
	}
	p := make([]byte, 0, size)
	p = append(p, recEntries)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(es)))
	for _, e := range es {
		p = binary.LittleEndian.AppendUint64(p, e.Index)
		p = binary.LittleEndian.AppendUint64(p, e.Term)
		p = append(p, byte(e.Type))
		p = binary.LittleEndian.AppendUint32(p, uint32(len(e.Data)))
		p = append(p, e.Data...)
	}
	return p
}

func decodeEntries(b []byte) ([]Entry, error) {
	if len(b) < 4 {
		return nil, errors.New("short entries")
	}
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	es := make([]Entry, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(b) < 21 {
			return nil, errors.New("short entry")
		}
		e := Entry{
			Index: binary.LittleEndian.Uint64(b),
			Term:  binary.LittleEndian.Uint64(b[8:]),
			Type:  EntryType(b[16]),
		}
		l := binary.LittleEndian.Uint32(b[17:])
		b = b[21:]
		if len(b) < int(l) {
			return nil, errors.New("short entry data")
		}
		if l > 0 {
			e.Data = append([]byte(nil), b[:l]...)
		}
		b = b[l:]
		es = append(es, e)
	}
	return es, nil
}

func encodeSnapshot(s *Snapshot) []byte {
	cfg, _ := json.Marshal(s.Config)
	p := make([]byte, 0, 8+8+4+len(cfg)+8+len(s.Data)+4)
	p = binary.LittleEndian.AppendUint64(p, s.Index)
	p = binary.LittleEndian.AppendUint64(p, s.Term)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(cfg)))
	p = append(p, cfg...)
	p = binary.LittleEndian.AppendUint64(p, uint64(len(s.Data)))
	p = append(p, s.Data...)
	return binary.LittleEndian.AppendUint32(p, crc32.ChecksumIEEE(p))
}

func readSnapshotFile(path string) (*Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) < 32 {
		return nil, errors.New("snapshot file corrupt (short)")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, errors.New("snapshot file corrupt (crc)")
	}
	s := &Snapshot{Index: binary.LittleEndian.Uint64(body), Term: binary.LittleEndian.Uint64(body[8:])}
	cl := binary.LittleEndian.Uint32(body[16:])
	body = body[20:]
	if err := json.Unmarshal(body[:cl], &s.Config); err != nil {
		return nil, err
	}
	body = body[cl:]
	dl := binary.LittleEndian.Uint64(body)
	s.Data = append([]byte(nil), body[8:8+dl]...)
	return s, nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync() // make the rename itself durable
}
