package log

import (
	"fmt"
	"os"
	"path"

	"github.com/golang/protobuf/proto"

	api "github.com/kazukousen/go-distributed/api/v1"
)

type segment struct {
	store                        *store
	index                        *index
	baseOffset, nextOffset       uint64
	maxStoreBytes, maxIndexBytes uint64
}

func newSegment(dir string, baseOffset uint64, maxStoreBytes, maxIndexBytes uint64) (*segment, error) {

	sf, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	store, err := newStore(sf)
	if err != nil {
		return nil, err
	}

	idxf, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	idx, err := newIndex(idxf, int64(maxIndexBytes))
	if err != nil {
		return nil, err
	}

	nextOffset := baseOffset
	if off, _, err := idx.Last(); err == nil {
		nextOffset = baseOffset + uint64(off) + 1
	}

	return &segment{
		store:         store,
		index:         idx,
		baseOffset:    baseOffset,
		nextOffset:    nextOffset,
		maxStoreBytes: maxStoreBytes,
		maxIndexBytes: maxIndexBytes,
	}, nil
}

func (s *segment) Append(record *api.Record) (off uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err := s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(uint32(off - s.baseOffset))
	if err != nil {
		return nil, fmt.Errorf("index failed: %w", err)
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, fmt.Errorf("store failed: %w", err)
	}

	r := &api.Record{}
	if err := proto.Unmarshal(p, r); err != nil {
		return nil, err
	}

	return r, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.maxStoreBytes || s.index.size >= s.maxIndexBytes
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := s.index.Remove(); err != nil {
		return err
	}

	if err := s.store.Remove(); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
