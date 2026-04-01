package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testBucket = "phase1-bucket"

func newTestDiskStore(t *testing.T) *DiskStore {
	t.Helper()

	s, err := openDiskStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	return s
}

func countFiles(root string) (int, error) {
	total := 0
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		total++
		return nil
	})
	return total, err
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newTestDiskStore(t)
	ctx := context.Background()

	if err := s.CreateBucket(ctx, testBucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	wantBody := []byte("mini-s3 phase1 round-trip payload")
	putMeta, err := s.Put(ctx, testBucket, "docs/readme.txt", bytes.NewReader(wantBody), "text/plain")
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	r, getMeta, err := s.Get(ctx, testBucket, "docs/readme.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r.Close()

	gotBody, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if !bytes.Equal(gotBody, wantBody) {
		t.Fatalf("body mismatch: got %q want %q", string(gotBody), string(wantBody))
	}
	if putMeta.ContentHash != getMeta.ContentHash {
		t.Fatalf("content hash mismatch: put=%s get=%s", putMeta.ContentHash, getMeta.ContentHash)
	}
	if putMeta.ETag != getMeta.ETag {
		t.Fatalf("etag mismatch: put=%s get=%s", putMeta.ETag, getMeta.ETag)
	}
	if len(putMeta.ContentHash) != 64 {
		t.Fatalf("unexpected content hash length: got %d want 64", len(putMeta.ContentHash))
	}
	if len(putMeta.ETag) != 32 {
		t.Fatalf("unexpected etag length: got %d want 32", len(putMeta.ETag))
	}
}

func TestCASDedupSingleBlobForIdenticalContent(t *testing.T) {
	s := newTestDiskStore(t)
	ctx := context.Background()

	if err := s.CreateBucket(ctx, testBucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	body := []byte("same payload for dedup")
	metaA, err := s.Put(ctx, testBucket, "k1", bytes.NewReader(body), "application/octet-stream")
	if err != nil {
		t.Fatalf("put k1: %v", err)
	}
	metaB, err := s.Put(ctx, testBucket, "k2", bytes.NewReader(body), "application/octet-stream")
	if err != nil {
		t.Fatalf("put k2: %v", err)
	}

	if metaA.ContentHash != metaB.ContentHash {
		t.Fatalf("expected same content hash, got %s and %s", metaA.ContentHash, metaB.ContentHash)
	}
	if metaA.ETag != metaB.ETag {
		t.Fatalf("expected same etag, got %s and %s", metaA.ETag, metaB.ETag)
	}

	blobCount, err := countFiles(filepath.Join(s.dataDir, "objects"))
	if err != nil {
		t.Fatalf("count blob files: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("expected exactly one blob file, got %d", blobCount)
	}

	if _, err := os.Stat(blobPath(s.dataDir, metaA.ContentHash)); err != nil {
		t.Fatalf("expected deduped blob to exist: %v", err)
	}
}

func TestGCOrphansRemovesPlantedOrphanBlob(t *testing.T) {
	s := newTestDiskStore(t)
	ctx := context.Background()

	if err := s.CreateBucket(ctx, testBucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	refMeta, err := s.Put(ctx, testBucket, "keep", bytes.NewReader([]byte("referenced")), "text/plain")
	if err != nil {
		t.Fatalf("put referenced object: %v", err)
	}

	orphanHash := strings.Repeat("a", 64)
	if orphanHash == refMeta.ContentHash {
		orphanHash = strings.Repeat("b", 64)
	}
	orphanPath := blobPath(s.dataDir, orphanHash)

	if err := os.MkdirAll(filepath.Dir(orphanPath), 0o750); err != nil {
		t.Fatalf("mkdir orphan shard: %v", err)
	}
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o640); err != nil {
		t.Fatalf("write orphan blob: %v", err)
	}

	if err := s.gcOrphans(); err != nil {
		t.Fatalf("gcOrphans: %v", err)
	}

	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("expected orphan blob removed, stat err=%v", err)
	}
	if _, err := os.Stat(blobPath(s.dataDir, refMeta.ContentHash)); err != nil {
		t.Fatalf("expected referenced blob to remain: %v", err)
	}
}

func TestOpenRemovesStaleTempFiles(t *testing.T) {
	dataDir := t.TempDir()

	if err := ensureDirs(dataDir); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}

	tmpPath := filepath.Join(dataDir, "tmp", "stale-upload")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0o640); err != nil {
		t.Fatalf("write stale tmp: %v", err)
	}

	s, err := openDiskStore(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	entries, err := os.ReadDir(filepath.Join(dataDir, "tmp"))
	if err != nil {
		t.Fatalf("read tmp dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected tmp dir to be empty on startup, found %d entries", len(entries))
	}
}

func TestConcurrentPutDistinctKeysNoIndexCorruption(t *testing.T) {
	s := newTestDiskStore(t)
	ctx := context.Background()

	if err := s.CreateBucket(ctx, testBucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const n = 50
	errCh := make(chan error, n)

	var wg sync.WaitGroup
	for i := range n {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			key := fmt.Sprintf("obj-%02d", i)
			body := []byte(fmt.Sprintf("payload-%02d", i))
			if _, err := s.Put(ctx, testBucket, key, bytes.NewReader(body), "application/octet-stream"); err != nil {
				errCh <- fmt.Errorf("put %s: %w", key, err)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	var putErrs []error
	for err := range errCh {
		putErrs = append(putErrs, err)
	}
	if len(putErrs) > 0 {
		t.Fatalf("parallel put errors: %v", putErrs)
	}

	for i := range n {
		key := fmt.Sprintf("obj-%02d", i)
		meta, err := s.Head(ctx, testBucket, key)
		if err != nil {
			t.Fatalf("head %s: %v", key, err)
		}
		if meta.Key != key {
			t.Fatalf("unexpected key in metadata: got %s want %s", meta.Key, key)
		}
	}

	objs, next, err := s.List(ctx, testBucket, "", "", 1000)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if next != "" {
		t.Fatalf("expected exhausted listing cursor, got %q", next)
	}
	if len(objs) != n {
		t.Fatalf("unexpected list length: got %d want %d", len(objs), n)
	}
}
