package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// offset is the index of the record i.e., the number corresponding to the record.
// position is the actual starting byte of the record.

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	// width is the number of bytes for each index in the index file
	width = offWidth + posWidth
)

// index contains a file, which records the indexes of each record.
// Each record is made up of the record's offset (record number) and its position (the actual byte it starts at).
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // size = max offset/index * width
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	// expand the file size before creating the memory map
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes in an offset (in) and returns the associated record's offset and position in the store.
// Offset is the number corresponding to the record.
// The reason we use uint32 for out is to save 4 bytes per index entry
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / width) - 1)
	} else {
		out = uint32(in)
	}

	posInIndexFile := uint64(out) * width
	if i.size < posInIndexFile+width {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[posInIndexFile : posInIndexFile+offWidth])
	pos = enc.Uint64(i.mmap[posInIndexFile+offWidth : posInIndexFile+width])
	return out, pos, nil
}

// Write appends offset and pos to the index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+width {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+width], pos)
	i.size += width
	return nil
}

func (i *index) Close() error {
	// sync the mmap with the file object
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// commit the content of the file to stable storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// the file has blank space, the last "index" recorded is not at the last 12 bytes of the file.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
