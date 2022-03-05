package log

import (
	"fmt"
	"os"
	"path"
)

// offset is unique identifier of a record which starts consecutively from 0.

type segment struct {
	store *store
	index *index
	// if baseOffset = x, it means the store holds records starting from x.
	// nextOffset refers to offset of the next record to be added.
	baseOffset, nextOffset uint64
	config                 Config
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