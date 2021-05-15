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
	recordLengthBytes = 8
)

type store struct {
	f    *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	return &store{
		f:    f,
		size: uint64(size),
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// write the record's length, so that we read the record,
	// we know how many bytes to read.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	pn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	n = uint64(pn) + recordLengthBytes
	s.size += n

	return n, pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// first flushes the writer buffer,
	//in case we're about to try to read a record that the buffer hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, recordLengthBytes)
	if _, err := s.f.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	dst := make([]byte, enc.Uint64(size))
	if _, err := s.f.ReadAt(dst, int64(pos+recordLengthBytes)); err != nil {
		return nil, err
	}

	return dst, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// first flushes the writer buffer,
	//in case we're about to try to read a record that the buffer hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.f.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.f.Close()
}

func (s *store) Remove() error {
	return os.Remove(s.f.Name())
}
