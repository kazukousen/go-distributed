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

	idx, err := newIndex(f, 1024)
	require.NoError(t, err)

	_, _, err = idx.Last()
	require.Error(t, err)

	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
	}

	for _, want := range entries {
		err := idx.Write(want.off, want.pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(want.off)
		require.NoError(t, err)
		require.Equal(t, want.pos, pos)
	}

	// when reading past existing entries, index and scanner should error
	_, _, err = idx.Read(uint32(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, 1024)
	require.NoError(t, err)
	off, pos, err := idx.Last()
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].pos, pos)
}
