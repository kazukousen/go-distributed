package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testRecord     = []byte("hello world")
	testRecordSize = uint64(len(testRecord)) + recordLengthBytes
)

func TestStore_AppendRead(t *testing.T) {

	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(testRecord)
		require.NoError(t, err)
		require.Equal(t, pos+n, testRecordSize*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	pos := uint64(0)
	for i := uint64(1); i < 4; i++ {
		b, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testRecord, b)
		pos += testRecordSize
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, recordLengthBytes)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, recordLengthBytes, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStore_Close(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(testRecord)
	require.NoError(t, err)

	beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (size int64, err error) {
	f, err := os.Open(name)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}
