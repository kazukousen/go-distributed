package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

const (
	indexOffsetWidth    = 4 // we store offsets as uint32s
	indexPositionsWidth = 8 // we store positions as uint64s
	indexEntireWidth    = indexOffsetWidth + indexPositionsWidth
)

type index struct {
	f    *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, maxIndexBytes int64) (*index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())

	if err := os.Truncate(f.Name(), maxIndexBytes); err != nil {
		return nil, err
	}

	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	idx := &index{
		f:    f,
		size: size,
		mmap: mmap,
	}
	return idx, nil
}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := idx.f.Sync(); err != nil {
		return err
	}

	if err := idx.f.Truncate(int64(idx.size)); err != nil {
		return err
	}

	return idx.f.Close()
}

func (idx *index) Read(at uint32) (off uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	return idx.read(at)
}

func (idx *index) Last() (off uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	return idx.read(uint32((idx.size / indexEntireWidth) - 1))
}

func (idx *index) read(at uint32) (off uint32, pos uint64, err error) {
	pos = uint64(at) * indexEntireWidth
	if idx.size < pos+indexEntireWidth {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(idx.mmap[pos : pos+indexOffsetWidth])
	pos = enc.Uint64(idx.mmap[pos+indexOffsetWidth : pos+indexEntireWidth])
	return off, pos, nil
}

func (idx *index) Write(off uint32, pos uint64) error {

	if uint64(len(idx.mmap)) < idx.size+indexEntireWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+indexOffsetWidth], off)
	enc.PutUint64(idx.mmap[idx.size+indexOffsetWidth:idx.size+indexEntireWidth], pos)

	idx.size += indexEntireWidth

	return nil
}

func (idx *index) Remove() error {
	return os.Remove(idx.f.Name())
}
