package coordinator

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Bucket and key rules are deliberately close to S3's so that clients written
// against S3 conventions behave predictably, but they are validated here (in
// the control plane) rather than only at the HTTP edge -- the gRPC API is a
// real API surface and must not trust its callers.
const (
	maxBucketLen = 63
	minBucketLen = 3
	maxKeyLen    = 1024
)

var (
	// ErrInvalidBucket rejects names that would be ambiguous or unsafe.
	ErrInvalidBucket = errors.New("invalid bucket name")
	// ErrInvalidKey rejects keys that would break path handling.
	ErrInvalidKey = errors.New("invalid object key")
)

// ValidateBucket enforces DNS-style bucket names.
func ValidateBucket(b string) error {
	if len(b) < minBucketLen || len(b) > maxBucketLen {
		return fmt.Errorf("%w: %q must be %d-%d characters", ErrInvalidBucket, b, minBucketLen, maxBucketLen)
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.'
		if !ok {
			return fmt.Errorf("%w: %q may only contain lowercase letters, digits, '-' and '.'", ErrInvalidBucket, b)
		}
	}
	if b[0] == '-' || b[0] == '.' || b[len(b)-1] == '-' || b[len(b)-1] == '.' {
		return fmt.Errorf("%w: %q must start and end with a letter or digit", ErrInvalidBucket, b)
	}
	if strings.Contains(b, "..") {
		return fmt.Errorf("%w: %q must not contain '..'", ErrInvalidBucket, b)
	}
	return nil
}

// ValidateKey enforces object key constraints.
//
// The traversal checks matter because keys are echoed into log lines, cache
// keys and (in future milestones) filesystem-backed exports. Chunk IDs -- not
// keys -- determine on-disk paths today, but rejecting "../" here keeps that
// property from becoming load-bearing by accident.
func ValidateKey(k string) error {
	if k == "" {
		return fmt.Errorf("%w: key must not be empty", ErrInvalidKey)
	}
	if len(k) > maxKeyLen {
		return fmt.Errorf("%w: key exceeds %d bytes", ErrInvalidKey, maxKeyLen)
	}
	if !utf8.ValidString(k) {
		return fmt.Errorf("%w: key must be valid UTF-8", ErrInvalidKey)
	}
	if strings.HasPrefix(k, "/") {
		return fmt.Errorf("%w: key must not start with '/'", ErrInvalidKey)
	}
	for _, seg := range strings.Split(k, "/") {
		if seg == ".." {
			return fmt.Errorf("%w: key must not contain '..' path segments", ErrInvalidKey)
		}
	}
	for _, r := range k {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: key must not contain control characters", ErrInvalidKey)
		}
	}
	return nil
}
