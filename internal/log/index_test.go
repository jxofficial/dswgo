package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	require.Equal(t, f.Name(), idx.Name())

	_, _, err = idx.Read(-1)
	require.Error(t, err)

	indexEntries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	// test that the index entries written can be read correctly
	for _, e := range indexEntries {
		err = idx.Write(e.Off, e.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(e.Off))
		require.NoError(t, err)
		require.Equal(t, e.Pos, pos)
	}

	// index record not found
	_, _, err = idx.Read(123)
	require.Equal(t, io.EOF, err)

	err = idx.Close()
	require.NoError(t, err)

	// index should take its state from the file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	off, pos, err := idx.Read(-1)
	// last index record offset and pos should be 1 and 10 respectively
	require.Equal(t, uint32(1), off)
	require.Equal(t, uint64(10), pos)
}
