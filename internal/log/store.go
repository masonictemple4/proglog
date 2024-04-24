package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// encoding for record size and index entries.
	enc = binary.BigEndian
)

const (
	// Number of bytes used to store a record's length
	lenWidth = 8
)

// store is the file that stores the record data.
type store struct {
	*os.File

	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists bytes passed to it in the store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Start of our data record.
	pos = s.size

	// Write data size so we know how much to read when reading.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write actual data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Increment bytes written to include data size we wrote at the beginning.
	w += lenWidth

	// Set the new position.
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make sure any buffered data is written.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// Implements the io.ReaderAt on store. Reads len(p)
// bytes into p beginning at the offset.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before
// closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
