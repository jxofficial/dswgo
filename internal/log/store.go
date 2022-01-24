package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenNumBits = 8
)

// store contains two methods to append and read bytes to and from the file
type store struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer // we write to buffered writer instead of file to reduce system calls.
	size uint64
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size // start appending from pos

	// Write the length of the record into the buffer so that
	// when we read, we know how many bytes to read.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// nn is the number of bytes written to s.buf from p
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	nn += lenNumBits
	s.size += uint64(nn)
	// return num bytes written, position which we started appending, and error if exists
	return uint64(nn), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// flush the buffer to prevent the case where we read a record that the buffer has not flushed to disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenNumBits)
	// read size of record from file, where size is a slice of bytes
	if _, err := s.file.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// convert the size, represented as slice of bytes, into int64
	b := make([]byte, enc.Uint64(size))
	// read record from file into b (byte slice)
	if _, err := s.file.ReadAt(b, int64(pos+lenNumBits)); err != nil {
		return nil, err
	}
	return b, nil
}

func newStore(f *os.File) (*store, error) {
	// get file's current size, in case the file already contains data
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		file: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}
