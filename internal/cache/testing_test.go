package cache

import (
	"io"
	"log/slog"
)

// testLogger discards output so failing tests are readable.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
