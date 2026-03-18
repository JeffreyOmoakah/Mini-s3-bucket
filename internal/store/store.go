package store

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
    ErrNotFound       = errors.New("store: object not found")
    ErrBucketNotFound = errors.New("store: bucket not found")
    ErrBucketExists   = errors.New("store: bucket already exists")
)

// ObjectMeta holds the metadata record for a stored object.
// The blob lives on disk at a path derived from ContentHash.
// This struct is what gets serialised into bbolt and returned to callers —
// it never holds the actual bytes.
type ObjectMeta struct {
    Bucket      string    `json:"bucket"`
    Key         string    `json:"key"`
    ContentHash string    `json:"content_hash"` // SHA-256 hex — the CAS address on disk
    ETag        string    `json:"etag"`          // MD5 hex — for S3 If-None-Match headers
    Size        int64     `json:"size"`
    ContentType string    `json:"content_type"`
    CreatedAt   time.Time `json:"created_at"`
}

// ObjectStore is the single abstraction the rest of the system uses.
// Every method must be safe for concurrent use.
// The concrete implementation is DiskStore — callers should hold this interface,
// not *DiskStore, so tests can inject fakes later.
type ObjectStore interface {
    CreateBucket(ctx context.Context, name string) error
    DeleteBucket(ctx context.Context, name string) error

    Put(ctx context.Context, bucket, key string, r io.Reader, contentType string) (ObjectMeta, error)
    Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectMeta, error)
    Head(ctx context.Context, bucket, key string) (ObjectMeta, error)
    Delete(ctx context.Context, bucket, key string) error

    // List returns up to limit objects whose keys start with prefix.
    // Pass cursor="" to start from the beginning. The second return value
    // is the next cursor — pass it back to get the next page.
    // Returns ("", nil) when exhausted.
    List(ctx context.Context, bucket, prefix, cursor string, limit int) ([]ObjectMeta, string, error)

    Close() error
}