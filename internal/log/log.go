package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/jxofficial/proglog/api/v1"
)

type Log struct {
	// Dir stores the segments
	// todo: give example of store and index file names
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
	// files include both index and store files
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
		// skip the store/index file to prevent double counting the offset
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Append(r *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(r)
	if err != nil {
		return 0, err
	}
	// the index is specific about how many index entries can be written,
	// given that each index entry is a fixed size of 12 bytes (index.indexEntryWidth).
	// i.e., if we make MaxIndexBytes a multiple of index.indexEntryWidth,
	// there should never be an overflow.
	// However, the store might occasionally exceed MaxStoreBytes
	// as there is no specific cap on the record data's size.
	if l.activeSegment.IsMaxed() {
		// subsequent records will belong to the new segment.
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var segment *segment
	// find the segment to read from
	for _, s := range l.segments {
		if s.baseOffset <= off && off < s.nextOffset {
			segment = s
			break
		}
	}
	// second condition technically should not happen.
	// It means that offset is out of bounds of the segment.
	if segment == nil || segment.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return segment.Read(off)
}

// Close iterates over all the segments and closes them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes the log and removes all its data.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes a log and creates a new log to replace it.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset returns the smallest offset in the Log.
// i.e., the earliest store record.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the largest offset in the Log.
// i.e., the most recent store record.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	// case where there are no records in the segment.
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// newSegment creates and appends a new segment to the log's segments,
// and sets the newly created segment as the active segment.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
