// Package blob is a content-addressed file store for uploaded recordings.
// Objects are named by their SHA-256, so identical uploads are stored once
// and the hash doubles as the result-cache key. In Kubernetes the directory
// is a shared volume; an object-storage backend would implement the same
// three methods.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Store struct {
	Dir string
}

// Put streams r to disk while hashing it, then moves it into place under its
// hash. It never holds the whole upload in memory.
func (s *Store) Put(r io.Reader) (sum string, size int64, err error) {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return "", 0, err
	}
	tmp, err := os.CreateTemp(s.Dir, "upload-*.part")
	if err != nil {
		return "", 0, err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	h := sha256.New()
	size, err = io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		tmp.Close()
		return "", 0, err
	}
	if err := tmp.Close(); err != nil {
		return "", 0, err
	}
	sum = hex.EncodeToString(h.Sum(nil))
	dst := s.path(sum)
	if _, err := os.Stat(dst); err == nil {
		return sum, size, nil // already stored
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", 0, err
	}
	return sum, size, nil
}

// Open returns the object for reading.
func (s *Store) Open(sum string) (*os.File, error) {
	if len(sum) != 64 {
		return nil, fmt.Errorf("bad blob id %q", sum)
	}
	f, err := os.Open(s.path(sum))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("blob %s: %w", sum, err)
	}
	return f, err
}

func (s *Store) path(sum string) string {
	return filepath.Join(s.Dir, sum[:2], sum[2:4], sum)
}
