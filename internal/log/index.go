package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// Length of offset record entries in our index.
	// These are stored as uint32s
	offWidth uint64 = 4
	// Length of position entry in our index.
	// These are stored as uint64s
	posWidth uint64 = 8
	// Use to jump straight to the position
	// of an entry given its offset.
	// (Position in file is: offset * entWidth)
	entWidth = offWidth + posWidth
)

// index defines our index file, which consists of a
// persisted file and a memory mapped file.
type index struct {
	// persisted file
	file *os.File
	// memory mapped file for performant operations.
	mmap gommap.MMap
	// Tells us the size of the index, and where to write
	// the next.
	size uint64
}

// creates an index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// Grow file to max index size before memory-mapping.
	// This can only be done once so we have to do it here.
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// Memory map our index file.
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close ensures the memory-mapped file has synced it's data
// with the persisted file and flushes the files contents to
// stable storage. Finally truncating the persisted file
// to how much data is actually in it.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read takes an offset and returns the records associated position
// in the store.
// The given offset is relative to the segment's base offset; Using
// relative offsets reduce the size of the indexes by storing them as
// uint32s.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// Check for last
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// Set position to the given offset.
	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// Get offset
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// Get position
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	// Do we have enough space?
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write the offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// Write the position.
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += uint64(entWidth)

	return nil
}
