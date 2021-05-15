package log

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/kazukousen/go-distributed/api/v1"
)

type Log struct {
	mu                                          sync.RWMutex
	dir                                         string
	activeSegment                               *segment
	segments                                    []*segment
	initialOffset, maxStoreBytes, maxIndexBytes uint64
}

func NewLog(dir string, initialOffset, maxStoreBytes, maxIndexBytes uint64) (*Log, error) {
	if maxStoreBytes == 0 {
		maxStoreBytes = 1024
	}
	if maxIndexBytes == 0 {
		maxIndexBytes = 1024
	}

	l := &Log{
		dir:           dir,
		initialOffset: initialOffset,
		maxStoreBytes: maxStoreBytes,
		maxIndexBytes: maxIndexBytes,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		// when the log is new and has no existing segments, bootstrap the initial segment.
		return l.newSegment(l.initialOffset)
	}

	baseOffsets := make([]uint64, len(files))
	for i, f := range files {
		offStr := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets[i] = off
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i += 2 {
		// baseOffset contains duplicate for store and index
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	seg, err := newSegment(l.dir, off, l.maxStoreBytes, l.maxIndexBytes)
	if err != nil {
		return err
	}

	l.activeSegment = seg
	l.segments = append(l.segments, seg)
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off <= seg.nextOffset {
			s = seg
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

// Truncate removes all segments whose highest offset is lower than `lowest`.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, seg := range l.segments {
		if latestOffset := seg.nextOffset - 1; latestOffset <= lowest {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}

	l.segments = segments

	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, seg := range l.segments {
		readers[i] = &originReader{store: seg.store, off: 0}
	}
	return io.MultiReader(readers...)
}

func (l *Log) LowerOffset() uint64 {
	return l.segments[0].baseOffset
}

func (l *Log) HigherOffset() uint64 {
	off := l.segments[len(l.segments)-1].nextOffset

	if off == 0 {
		return 0
	}

	return off - 1
}

type originReader struct {
	store *store
	off   int64
}

func (r *originReader) Read(p []byte) (n int, err error) {
	n, err = r.store.ReadAt(p, r.off)
	r.off += int64(n)
	return
}
