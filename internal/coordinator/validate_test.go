package coordinator

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateBucket(t *testing.T) {
	valid := []string{"abc", "my-bucket", "photos.2026", "a1b2c3", strings.Repeat("a", 63)}
	for _, b := range valid {
		if err := ValidateBucket(b); err != nil {
			t.Errorf("ValidateBucket(%q) rejected a valid name: %v", b, err)
		}
	}

	invalid := map[string]string{
		"":                      "empty",
		"ab":                    "too short",
		strings.Repeat("a", 64): "too long",
		"MyBucket":              "uppercase",
		"my_bucket":             "underscore",
		"-bucket":               "leading dash",
		"bucket-":               "trailing dash",
		".bucket":               "leading dot",
		"bucket.":               "trailing dot",
		"my..bucket":            "consecutive dots",
		"my/bucket":             "slash",
		"my bucket":             "space",
		"../etc":                "traversal",
	}
	for b, why := range invalid {
		err := ValidateBucket(b)
		if err == nil {
			t.Errorf("ValidateBucket(%q) accepted an invalid name (%s)", b, why)
			continue
		}
		if !errors.Is(err, ErrInvalidBucket) {
			t.Errorf("ValidateBucket(%q) should wrap ErrInvalidBucket, got %v", b, err)
		}
	}
}

func TestValidateKey(t *testing.T) {
	valid := []string{
		"file.bin",
		"photos/2026/cat.jpg",
		"deeply/nested/path/with/many/segments",
		"unicode-ключ-🎉",
		"a..b", // dots inside a segment are fine
		"...",  // not a bare ".." segment
		strings.Repeat("k", 1024),
	}
	for _, k := range valid {
		if err := ValidateKey(k); err != nil {
			t.Errorf("ValidateKey(%q) rejected a valid key: %v", k, err)
		}
	}

	invalid := map[string]string{
		"":                        "empty",
		strings.Repeat("k", 1025): "too long",
		"/leading-slash":          "leading slash",
		"../escape":               "traversal segment",
		"a/../b":                  "embedded traversal",
		"a/..":                    "trailing traversal",
		"bad\x00key":              "NUL byte",
		"bad\nkey":                "newline",
		"bad\x7fkey":              "DEL",
	}
	for k, why := range invalid {
		err := ValidateKey(k)
		if err == nil {
			t.Errorf("ValidateKey(%q) accepted an invalid key (%s)", k, why)
			continue
		}
		if !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ValidateKey(%q) should wrap ErrInvalidKey, got %v", k, err)
		}
	}
}

func TestValidateKeyRejectsInvalidUTF8(t *testing.T) {
	// Keys end up in logs, JSON responses and cache keys; invalid UTF-8 would
	// corrupt all three.
	if err := ValidateKey(string([]byte{0xff, 0xfe})); err == nil {
		t.Fatal("expected invalid UTF-8 to be rejected")
	}
}
