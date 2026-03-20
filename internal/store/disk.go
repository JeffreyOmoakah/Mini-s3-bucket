package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
	
	bolt "go.etcd.io/bbolt"
)

// DiskStore is the Phase 1 single-node object store implementation.
//
// Concurrency model: bbolt handles its own locking — all transactions are
// serialised at the DB level. The disk I/O in Put is concurrent-safe because
// each write uses a unique temp file, and os.Rename is atomic at the OS level.
// Multiple goroutines can Put to different keys simultaneously.
type DiskStore struct {
    dataDir string
    db      *bolt.DB
}

// Open creates or reopens a DiskStore at the given directory.
// Creates the full directory skeleton if it doesn't exist.
func Open(dataDir string) (*DiskStore, error) {
    if err := ensureDirs(dataDir); err != nil {
        return nil, err
    }

    dbPath := filepath.Join(dataDir, "store.db")
    db, err := bolt.Open(dbPath, 0o640, &bolt.Options{
        Timeout: 2 * time.Second, // fail fast if another process holds the lock
    })
    if err != nil {
        return nil, fmt.Errorf("store: open bolt db at %s: %w", dbPath, err)
    }

    // Create bbolt namespaces in a single write transaction.
    // db.Update is synchronous and returns only after the transaction commits.
    if err := db.Update(initSchema); err != nil {
        db.Close()
        return nil, err
    }

    s := &DiskStore{dataDir: dataDir, db: db}

    // GC any leftover tmp files from a previous crash before accepting requests.
    if err := s.gcTmp(); err != nil {
        db.Close()
        return nil, err
    }

    return s, nil
}

// Close flushes the bbolt write-ahead log and releases the file lock.
func (s *DiskStore) Close() error {
    return s.db.Close()
}

// ─── Bucket operations ────────────────────────────────────────────────────────

func (s *DiskStore) CreateBucket(ctx context.Context, name string) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        bkts := tx.Bucket(bktBuckets)
        if bkts.Get([]byte(name)) != nil {
            return ErrBucketExists
        }
        rec := bucketRecord{
            Name:      name,
            CreatedAt: time.Now().UTC().Format(time.RFC3339),
        }
        data, err := encodeBucketRecord(rec)
        if err != nil {
            return err
        }
        return bkts.Put([]byte(name), data)
    })
}

func (s *DiskStore) DeleteBucket(ctx context.Context, name string) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        bkts := tx.Bucket(bktBuckets)
        if bkts.Get([]byte(name)) == nil {
            return ErrBucketNotFound
        }

        // Sweep the objects namespace and delete every entry belonging to this bucket.
        // The blob files become orphans — a full GC pass (Phase 7 leader job)
        // will reclaim them. For Phase 1 you can call gcOrphans() explicitly after
        // if you want immediate reclaim.
        objs := tx.Bucket(bktObjects)
        prefix := indexPrefix(name)
        c := objs.Cursor()
        for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
            if err := objs.Delete(k); err != nil {
                return fmt.Errorf("store: delete object index entry: %w", err)
            }
        }

        return bkts.Delete([]byte(name))
    })
}

// ─── Put ──────────────────────────────────────────────────────────────────────

// Put streams the contents of r into the store.
//
// This is the most critical method — read the write path diagram before
// modifying this function. The ordering of steps is load-bearing for
// crash safety. Do not reorder steps 3-5.
func (s *DiskStore) Put(
    ctx context.Context,
    bucket, key string,
    r io.Reader,
    contentType string,
) (ObjectMeta, error) {

    // ── Step 1: validate bucket (read txn, cheap) ──────────────────────────
    if err := s.db.View(func(tx *bolt.Tx) error {
        if tx.Bucket(bktBuckets).Get([]byte(bucket)) == nil {
            return ErrBucketNotFound
        }
        return nil
    }); err != nil {
        return ObjectMeta{}, err
    }

    // ── Step 2: stream r → temp file, computing both hashes in one pass ────
    //
    // io.TeeReader / io.MultiWriter lets us hash and write in a single read
    // of r. We never buffer the full body in memory — r might be 100GB.
    //
    //   r → MultiWriter → [sha256, md5, tmp file]
    //
    // SHA-256 is our CAS address (collision-resistant, used for dedup and
    // integrity). MD5 is our ETag (S3 compatibility — weak but expected by
    // every S3 client library).
    tmp, err := openTemp(s.dataDir)
    if err != nil {
        return ObjectMeta{}, err
    }
    tmpName := tmp.Name()

    // cleanup is called on any error path after the temp file is created.
    // We define it as a named func so it's called consistently.
    cleanup := func() { os.Remove(tmpName) }

    sha := sha256.New()
    md5h := md5.New()
    mw := io.MultiWriter(tmp, sha, md5h)

    size, err := io.Copy(mw, r)
    if err != nil {
        tmp.Close()
        cleanup()
        return ObjectMeta{}, fmt.Errorf("store: stream body: %w", err)
    }

    // ── Step 3: fsync before rename ─────────────────────────────────────────
    //
    // Without fsync, the OS may still hold dirty pages in memory. A crash
    // after rename but before the pages flush would leave us with a
    // zero-byte or corrupt file at the CAS path, with a valid index entry
    // pointing to it. fsync guarantees the bytes are on physical media before
    // we proceed. Yes, this makes writes slower — that's the price of durability.
    if err := tmp.Sync(); err != nil {
        tmp.Close()
        cleanup()
        return ObjectMeta{}, fmt.Errorf("store: fsync temp file: %w", err)
    }
    if err := tmp.Close(); err != nil {
        cleanup()
        return ObjectMeta{}, fmt.Errorf("store: close temp file: %w", err)
    }

    contentHash := hex.EncodeToString(sha.Sum(nil))
    etag := hex.EncodeToString(md5h.Sum(nil))
    dest := blobPath(s.dataDir, contentHash)

    // ── Step 4: atomic rename ───────────────────────────────────────────────
    //
    // os.Rename is a single syscall (rename(2) on Linux). The kernel either
    // completes it or it never happens — there is no partial state. This
    // means the blob at dest is either the previous content (if rename hasn't
    // run) or the new complete content (if it has). It can never be partial.
    //
    // Two concurrent Puts of the *same* content will race to rename the same
    // dest path. That's fine — both files contain identical bytes (same hash),
    // so whichever rename wins, the result is correct. The loser's temp file
    // gets overwritten and both callers proceed to write the same bbolt entry.
    if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
        cleanup()
        return ObjectMeta{}, fmt.Errorf("store: create shard dir: %w", err)
    }
    if err := os.Rename(tmpName, dest); err != nil {
        cleanup()
        return ObjectMeta{}, fmt.Errorf("store: rename to CAS path: %w", err)
    }

    // ── Step 5: persist metadata — the commit point ─────────────────────────
    //
    // The bbolt Update is the moment this object becomes visible to readers.
    // If this fails, the blob at dest is an orphan — it exists on disk but
    // no index entry points to it. Startup GC (gcOrphans) reclaims it.
    //
    // We do NOT attempt to remove dest on failure here, because a concurrent
    // Put of the same content may have succeeded and the file is legitimately
    // referenced by another key's index entry.
    meta := ObjectMeta{
        Bucket:      bucket,
        Key:         key,
        ContentHash: contentHash,
        ETag:        etag,
        Size:        size,
        ContentType: contentType,
        CreatedAt:   time.Now().UTC(),
    }

    if err := s.db.Update(func(tx *bolt.Tx) error {
        data, err := encodeObjectMeta(meta)
        if err != nil {
            return err
        }
        return tx.Bucket(bktObjects).Put(indexKey(bucket, key), data)
    }); err != nil {
        return ObjectMeta{}, fmt.Errorf("store: write index: %w", err)
    }

    return meta, nil
}

// ─── Get ──────────────────────────────────────────────────────────────────────

// Get returns a streaming reader and the object metadata.
// The caller MUST close the returned ReadCloser — it wraps an open *os.File.
// Failing to close it leaks a file descriptor.
func (s *DiskStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectMeta, error) {
    meta, err := s.Head(ctx, bucket, key)
    if err != nil {
        return nil, ObjectMeta{}, err
    }

    path := blobPath(s.dataDir, meta.ContentHash)
    f, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) {
            // Index entry exists but blob file is missing.
            // This indicates storage corruption — the index and disk are out of sync.
            // Return a wrapped ErrNotFound so callers can distinguish this from
            // "key never existed".
            return nil, ObjectMeta{}, fmt.Errorf(
                "store: blob missing for %s/%s (hash=%s), index corrupt: %w",
                bucket, key, meta.ContentHash, ErrNotFound,
            )
        }
        return nil, ObjectMeta{}, fmt.Errorf("store: open blob: %w", err)
    }

    return f, meta, nil
}

// ─── Head ─────────────────────────────────────────────────────────────────────

// Head returns metadata without opening the blob file.
// This is the hot path — S3 clients call HEAD before conditional GETs.
// It's a single bbolt read transaction, no disk seek.
func (s *DiskStore) Head(ctx context.Context, bucket, key string) (ObjectMeta, error) {
    var meta ObjectMeta
    err := s.db.View(func(tx *bolt.Tx) error {
        data := tx.Bucket(bktObjects).Get(indexKey(bucket, key))
        if data == nil {
            return ErrNotFound
        }
        var err error
        meta, err = decodeObjectMeta(data)
        return err
    })
    return meta, err
}

// ─── Delete ───────────────────────────────────────────────────────────────────

// Delete removes the object's index entry.
//
// The blob file is intentionally NOT deleted immediately for two reasons:
//  1. Another key may reference the same content hash (CAS dedup).
//  2. Removing files from the hot I/O path adds latency to the request.
//
// Instead, the blob becomes an orphan. Phase 7's leader GC job will call
// gcOrphans() periodically to sweep blobs with no index references.
// For Phase 1, you can call gcOrphans() after delete in tests if you need
// immediate reclaim.
func (s *DiskStore) Delete(ctx context.Context, bucket, key string) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        objs := tx.Bucket(bktObjects)
        ik := indexKey(bucket, key)
        if objs.Get(ik) == nil {
            return ErrNotFound
        }
        return objs.Delete(ik)
    })
}

// ─── List ─────────────────────────────────────────────────────────────────────

// List returns paginated object metadata for objects in bucket matching prefix.
//
// bbolt Cursor.Seek() positions the cursor at the first key >= our seek target
// in O(log n). We then iterate forward, which is O(k) for k results.
// This is not a full table scan — it's a range scan on the B+tree.
func (s *DiskStore) List(
    ctx context.Context,
    bucket, prefix, cursor string,
    limit int,
) ([]ObjectMeta, string, error) {

    if limit <= 0 || limit > 1000 {
        limit = 1000
    }

    var results []ObjectMeta
    var nextCursor string

    bucketPrefix := indexPrefix(bucket)
    objectPrefix := indexKey(bucket, prefix)

    // Where to start the cursor scan.
    // On first page: seek to the bucket+prefix boundary.
    // On subsequent pages: seek to the last-returned key, then skip it.
    seekKey := objectPrefix
    if cursor != "" {
        seekKey = indexKey(bucket, cursor)
    }

    err := s.db.View(func(tx *bolt.Tx) error {
        c := tx.Bucket(bktObjects).Cursor()

        for k, v := c.Seek(seekKey); k != nil; k, v = c.Next() {
            // Left the bucket's namespace entirely — stop.
            if !bytes.HasPrefix(k, bucketPrefix) {
                break
            }
            // Key doesn't match the user's prefix — stop.
            // (Because of lexicographic ordering, once we pass the prefix
            // we'll never come back to it, so breaking is safe.)
            if !bytes.HasPrefix(k, objectPrefix) {
                break
            }
            // On a cursor continuation, skip the key we already returned.
            if cursor != "" && bytes.Equal(k, seekKey) {
                continue
            }

            meta, err := decodeObjectMeta(v)
            if err != nil {
                return err
            }
            results = append(results, meta)

            if len(results) == limit {
                nextCursor = meta.Key
                break
            }
        }
        return nil
    })

    return results, nextCursor, err
}