package store 

import (
    "fmt"
    "os"
    "path/filepath"
)

// blobPath returns the absolute path for an object given its SHA-256 content hash.
//
// We shard by the first two hex characters of the hash — this gives 256
// subdirectories (00–ff), capping each at ~total_objects/256 files.
// Filesystems degrade when a single directory exceeds ~100k entries;
// this prevents that problem at any realistic scale for Phase 1.
//
//   hash "a3f4b91c8d..." → <dataDir>/objects/a3/f4b91c8d...
func blobPath(dataDir, hash string) string {
    if len(hash) < 4 {
        panic(fmt.Sprintf("store: content hash too short: %q", hash))
    }
    return filepath.Join(dataDir, "objects", hash[:2], hash[2:])
}

// ensureDirs creates the required directory skeleton under dataDir.
// Idempotent — safe to call every time Open() runs.
// We do NOT pre-create the 256 shard dirs — they're created lazily
// per-write with os.MkdirAll, which is cheap and idempotent.
func ensureDirs(dataDir string) error {
    dirs := []string{
        filepath.Join(dataDir, "objects"),
        filepath.Join(dataDir, "tmp"),
    }
    for _, d := range dirs {
        if err := os.MkdirAll(d, 0o750); err != nil {
            return fmt.Errorf("store: create dir %s: %w", d, err)
        }
    }
    return nil
}

// openTemp creates a uniquely-named temporary file in dataDir/tmp.
// The caller owns the file and must either rename it (success path)
// or remove it (error path).
func openTemp(dataDir string) (*os.File, error) {
    f, err := os.CreateTemp(filepath.Join(dataDir, "tmp"), "upload-*")
    if err != nil {
        return nil, fmt.Errorf("store: create temp file: %w", err)
    }
    return f, nil
}