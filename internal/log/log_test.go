package log

import (
	"io"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"

	api "github.com/kazukousen/go-distributed/api/v1"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, f := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testLog_AppendRead,
		"offset out of range error":         testLog_OutOfRangeErr,
		"init with existing segments":       testLog_InitExisting,
		"reader":                            testLog_Reader,
		"truncate":                          testLog_Truncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			l, err := NewLog(dir, 0, 32, 0)
			require.NoError(t, err)

			f(t, l)
		})
	}
}

func testLog_AppendRead(t *testing.T, l *Log) {
	in := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(in)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	out, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, in.Value, out.Value)
}

func testLog_OutOfRangeErr(t *testing.T, l *Log) {
	_, err := l.Read(1)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

// when a log is created, the log bootstraps itself from the data stored by prior log instances.
func testLog_InitExisting(t *testing.T, l *Log) {
	in := &api.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		off, err := l.Append(in)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}
	require.NoError(t, l.Close())

	lowest := l.LowerOffset()
	require.Equal(t, uint64(0), lowest)
	highest := l.HigherOffset()
	require.Equal(t, uint64(2), highest)

	newLog, err := NewLog(l.dir, l.initialOffset, l.maxStoreBytes, l.maxIndexBytes)
	require.NoError(t, err)
	lowest = newLog.LowerOffset()
	require.Equal(t, uint64(0), lowest)
	highest = newLog.HigherOffset()
	require.Equal(t, uint64(2), highest)
}

func testLog_Reader(t *testing.T, l *Log) {
	in := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(in)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	out := &api.Record{}
	err = proto.Unmarshal(b[recordLengthBytes:], out)
	require.NoError(t, err)
	require.Equal(t, in.Value, out.Value)
}

func testLog_Truncate(t *testing.T, l *Log) {
	in := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		off, err := l.Append(in)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}

	err := l.Truncate(1)
	require.NoError(t, err)

	_, err = l.Read(0)
	require.Error(t, err)
}
