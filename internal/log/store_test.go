package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	recordData = []byte("hello world")
	// recordLen is length of entire record, including the len prefix.
	recordLen = uint64(len(recordData)) + lenNumBytes
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// test that our service (store implementation) resumes reading from the latest record on service failure
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(recordData)
		require.NoError(t, err)
		require.Equal(t, recordLen*i, pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		rd, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, recordData, rd)
		pos += recordLen
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// read length into lenbs
		lenbs := make([]byte, lenNumBytes)
		n, err := s.ReadAt(lenbs, off) // n should be 8 bytes
		require.NoError(t, err)
		require.Equal(t, lenNumBytes, n)

		off += int64(n)

		// get size of record data by converting the value of the bytes in lenbs to uint64
		lenRecordData := enc.Uint64(lenbs)
		rd := make([]byte, lenRecordData)
		n, err = s.ReadAt(rd, off)
		require.NoError(t, err)
		require.Equal(t, recordData, rd)
		require.Equal(t, int(lenRecordData), n)

		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(recordData)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	// check that store persists the data left in the buffer when Close is called
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
	require.True(t, beforeSize == 0)
	require.True(t, afterSize == int64(recordLen))
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
