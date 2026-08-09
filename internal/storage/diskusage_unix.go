//go:build unix

package storage

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// diskUsage reports the filesystem's total and available bytes for the volume
// containing path.
func diskUsage(path string) (total, available int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	//nolint:unconvert // Bsize is int32 on some platforms, int64 on others.
	bsize := int64(st.Bsize)
	return int64(st.Blocks) * bsize, int64(st.Bavail) * bsize, nil
}

func diskAvailable(path string) (int64, error) {
	_, avail, err := diskUsage(path)
	return avail, err
}
