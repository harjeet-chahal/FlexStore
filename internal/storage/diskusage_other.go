//go:build !unix

package storage

import "errors"

// errUnsupported keeps the build honest on platforms where we have not
// implemented capacity reporting: nodes must be configured with an explicit
// FLEXSTORE_NODE_CAPACITY_BYTES rather than silently advertising zero.
var errUnsupported = errors.New("filesystem capacity reporting is not implemented on this platform; set FLEXSTORE_NODE_CAPACITY_BYTES")

func diskUsage(string) (int64, int64, error) { return 0, 0, errUnsupported }

func diskAvailable(string) (int64, error) { return 0, errUnsupported }
