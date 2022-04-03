package log

import (
	"io"
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
	// example of segments (files in Dir)
	// 0.store, 0.index, 3.store, ...
	// in this case, the first store file has records 0-2,
	// and the second store has records with offset 3 onwards.
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

// Append appends the record argument and returns the offset of the appended record.
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
	// The second condition technically should not happen.
	// It means that off is out of bounds of the segment.
	if segment == nil || segment.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
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

// Remove removes all the log's data and closes the log.
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

// Truncate removes all logs with offset lower than the lowest argument.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
		} else {
			segments = append(segments, s)
		}
	}
	l.segments = segments
	return nil
}

// Reader returns a Reader that is a sequential concatenation of all the log's segments' stores.
// The Reader is used to read the entire log.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originReader{s.store, 0}
	}
	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

type originReader struct {
	*store
	off int64 // off is the number of bytes that has been read from *store.
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

// setup assigns the log's segments and activeSegment.
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
