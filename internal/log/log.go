package log

import (
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	// Dir stores the segments
	Dir string
	Config
	mu            sync.RWMutex
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, f := range files {
		// remove file extension
		offsetStr := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
		// offset returned will be an int since bitsize 0 corresponds to int type
		offset, _ := strconv.ParseUint(offsetStr, 10, 0)
		baseOffsets = append(baseOffsets, offset)
	}
	// sort the offsets so that the segments can be created in order
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// todo: understand why we increment i
		// i++
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
