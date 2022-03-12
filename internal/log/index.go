package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// offset is the index of the store record i.e., the number corresponding to the store record.
// position is the actual starting byte of the record in the store file.

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	// indexEntryWidth is the number of bytes for each index in the index file
	indexEntryWidth = offWidth + posWidth
)

// index contains a file, which holds the indexes of each record.
// Each index is made up of the store record's offset (record number) and its position
// (the actual byte the record starts at in the store file).
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // size = max record offset * indexEntryWidth
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
// We use uint32 for out to save 4 bytes per index entry.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / indexEntryWidth) - 1)
	} else {
		out = uint32(in)
	}

	posInIndexFile := uint64(out) * indexEntryWidth
	if i.size < posInIndexFile+indexEntryWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[posInIndexFile : posInIndexFile+offWidth])
	pos = enc.Uint64(i.mmap[posInIndexFile+offWidth : posInIndexFile+indexEntryWidth])
	return out, pos, nil
}

// Write appends offset and pos to the index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+indexEntryWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+indexEntryWidth], pos)
	i.size += indexEntryWidth
	return nil
}

func (i *index) Close() error {
	// sync the mmap with the file object
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// commit the content of the file to persistent storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// the file has blank space due to the memory map,
	// the last index recorded is not at the last 12 bytes of the file.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
