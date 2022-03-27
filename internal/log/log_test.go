package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/jxofficial/proglog/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		// "append and read a record": testAppendRead,
		// "read out of range":        testReadOutOfRangeErr,
		"init existing log": testInitExistingLog,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(r)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	readRecord, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, r.Value, readRecord.Value)
}

func testReadOutOfRangeErr(t *testing.T, log *Log) {
	r, err := log.Read(1)
	require.Nil(t, r)
	require.Error(t, err)
}

func testInitExistingLog(t *testing.T, existingLog *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := existingLog.Append(r)
		require.NoError(t, err)
	}
	// require.NoError(t, existingLog.Close())

	lowest, err := existingLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)

	highest, err := existingLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)

	// newLog, err := NewLog(existingLog.Dir, existingLog.Config)

}
