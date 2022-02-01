package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// Record refers to RecordData + RecordLength (8 bytes).
// Size and length is used interchangeably.
// RecordData refers to only the raw data.

var (
	enc = binary.BigEndian
)

const (
	lenNumBytes = 8
)

// store implements two methods to append and read bytes to and from the file
type store struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer // we write to buffered writer instead of file to reduce system calls.
	size uint64        // size is the entire size of the file, ie the length of all records
}

// Append writes the bytes in p into the store.
// It returns num bytes written (inclusive of record length),
// the position which we started appending, and error if exists.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size // start appending from pos

	// Write the length of the record into the buffer so that
	// when we read, we know how many bytes to read.
	// record length is written in big endian encoding.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// nn is the number of bytes written to s.buf from p
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	nn += lenNumBytes
	s.size += uint64(nn)
	return uint64(nn), pos, nil
}

// Read returns the record data stored at the given position given a pos.
// pos is the index at which the record starts.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the buffer to prevent the case where we read a record that the buffer has not flushed to disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenNumBytes)
	// read record size from file, where size is a slice of bytes
	if _, err := s.file.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// convert the size, represented as slice of bytes, into int64
	recordData := make([]byte, enc.Uint64(size))

	// read record from file into recordData (byte slice)
	if _, err := s.file.ReadAt(recordData, int64(pos+lenNumBytes)); err != nil {
		return nil, err
	}
	return recordData, nil
}

// ReadAt reads len(p) bytes into p starting from the given offset in the store's file.
// It returns the number of bytes n read into p, if n < len(p), an error will be returned.
// It implements io.ReaderAt.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.file.ReadAt(p, off)
}

// Close persists any data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
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
