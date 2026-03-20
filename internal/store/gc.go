package store

import (
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

// gcTmp removes all files from dataDir/tmp.
// Any file there at startup is guaranteed to be an incomplete upload
// (the process crashed mid-write). This runs synchronously before
// the store accepts requests.
func (s *DiskStore) gcTmp() error {
    tmpDir := filepath.Join(s.dataDir, "tmp")
    entries, err := os.ReadDir(tmpDir)
    if err != nil {
        return fmt.Errorf("store: read tmp dir: %w", err)
    }
    for _, e := range entries {
        if e.IsDir() {
            continue
        }
        path := filepath.Join(tmpDir, e.Name())
        if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
            return fmt.Errorf("store: remove tmp file %s: %w", path, err)
        }
    }
    return nil
}

// gcOrphans walks every blob file under dataDir/objects and removes any
// whose content hash has no corresponding entry in the bbolt index.
//
// An orphan arises when Put's bbolt write fails after the os.Rename succeeds.
// This is a safe, idempotent operation — it only removes files that are
// provably unreferenced.
//
// For Phase 1 you can call this manually after tests or DeleteBucket.
// In Phase 7, the cluster leader will schedule this periodically.
func (s *DiskStore) gcOrphans() error {
    objectsDir := filepath.Join(s.dataDir, "objects")

    // Build the complete set of content hashes referenced in the index.
    // For Phase 1 (single node, bounded dataset) this fits in memory.
    // Phase 7 will need a streaming/incremental approach for large clusters.
    referencedHashes := make(map[string]struct{})

    if err := s.db.View(func(tx *bolt.Tx) error {
        return tx.Bucket(bktObjects).ForEach(func(_, v []byte) error {
            meta, err := decodeObjectMeta(v)
            if err != nil {
                return err
            }
            referencedHashes[meta.ContentHash] = struct{}{}
            return nil
        })
    }); err != nil {
        return fmt.Errorf("store: build referenced hash set: %w", err)
    }

    // Walk shard directories and remove unreferenced blobs.
    shards, err := os.ReadDir(objectsDir)
    if err != nil {
        return fmt.Errorf("store: read objects dir: %w", err)
    }

    for _, shard := range shards {
        if !shard.IsDir() {
            continue
        }
        shardPath := filepath.Join(objectsDir, shard.Name())
        blobs, err := os.ReadDir(shardPath)
        if err != nil {
            return fmt.Errorf("store: read shard dir %s: %w", shardPath, err)
        }
        for _, blob := range blobs {
            // Reconstruct the full hash: shard dir name + blob file name
            fullHash := shard.Name() + blob.Name()
            if _, ok := referencedHashes[fullHash]; ok {
                continue // referenced — keep it
            }
            blobPath := filepath.Join(shardPath, blob.Name())
            if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
                return fmt.Errorf("store: remove orphan blob %s: %w", blobPath, err)
            }
        }
    }

    return nil
}