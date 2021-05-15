package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/kazukousen/go-distributed/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("hello world"),
	}

	s, err := newSegment(dir, 16, 1024, indexEntireWidth*3)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	// resize
	s, err = newSegment(dir, 16, uint64(len(want.Value)*3), 1024)
	require.NoError(t, err)

	// maxed store
	require.True(t, s.IsMaxed())

	// remove to reset
	err = s.Remove()
	s, err = newSegment(dir, 16, 1024, 1024)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
