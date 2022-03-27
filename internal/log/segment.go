package log

import (
	"fmt"
	"os"
	"path"

	"github.com/golang/protobuf/proto"

	api "github.com/jxofficial/proglog/api/v1"
)

// offset starts from 0 and increments consecutively. It is a unique identifier of a record.

type segment struct {
	store *store
	index *index
	// if baseOffset = x, it means the store for this segment holds records
	// with record numbers starting from x. i.e., it is the offset from the base store record (0),
	// nextOffset refers to offset of the next record to be added to this segment's store.
	baseOffset, nextOffset uint64
	config                 Config
}

// Append appends a record to the store and writes the corresponding index entry.
// It returns the offset of the appended record, and err if any.
func (s *segment) Append(r *api.Record) (offset uint64, err error) {
	curr := s.nextOffset
	r.Offset = curr

	p, err := proto.Marshal(r)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	indexRelativeOffset := s.nextOffset - s.baseOffset
	err = s.index.Write(uint32(indexRelativeOffset), pos)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return curr, nil
}

// Read takes in the segment's index's relative offset and returns the corresponding *api.Record.
func (s *segment) Read(off uint64) (*api.Record, error) {
	indexRelativeOffset := int64(off - s.baseOffset)
	_, pos, err := s.index.Read(indexRelativeOffset)
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size
// which occurs when either the index or the store cannot hold any more bytes.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close closes the index and store files and flushes the data into persistent storage,
// i.e. the respective index and store files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// creating the store
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	// creating the index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	// if index is empty, it means the next offset is the same as the segment's base offset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		// add base + relative offset
		// eg if segment starts from record 10, and index file alr has two records of relative offset 0 and 1,
		// the next record to be added has an offset of 10 + 1 + 1 = 12.
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j
// e.g. nearestMultiple(9,4) returns 8.
// k is assumed to be positive and nonzero.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	// todo: figure out the logic behind this
	return ((j - k + 1) / k) * k
}
