package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/masonictemple4/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// Segment wraps the `store` and `index` types
// so that we may use them to read and write
// to our log (active segment)
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, conf Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     conf,
	}

	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, conf); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record segment and returns the newly appended
// records offset. The log returns the records offset to the API response.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	curOff := s.nextOffset
	record.Offset = curOff

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	relOff := uint32(s.nextOffset - uint64(s.baseOffset))
	if err = s.index.Write(relOff, pos); err != nil {
		return 0, err
	}

	s.nextOffset++
	return curOff, nil
}

// Read returns the record for the given offset. Similar to writes
// to read a record the segment must first translate absolute index into
// a relative offset and get the associated index entry.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		fmt.Printf("error reading in segment - read after index: %v\n", err)
		return nil, err
	}

	fmt.Printf("segment index read data: %d\n", pos)

	rData, err := s.store.Read(pos)
	if err != nil {
		fmt.Printf("error reading in segment - read after store: %v\n", err)
		return nil, err
	}

	fmt.Printf("segment read data: %s\n", string(rData))

	record := &api.Record{}
	err = proto.Unmarshal(rData, record)

	return record, err
}

// IsMaxed returns wether the segment has reached its max size.
// Either the store or index.
// Can be used to know it needs to create a new segment.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

// Close will gracefully shutdown the segment's index and
// store or return an error.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns nearest and lesser multiple of k in j
// you can take this to make sure we stay under the user's disk
// capacity.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
