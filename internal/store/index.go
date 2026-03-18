package store

import (
	"encoding/json"
	"fmt"
	
	bolt "go.etcd.io/bbolt"
)

// bbolt bucket names — using short byte slices to save repeated allocations.
// These are the namespace keys inside the bolt DB file, not S3 buckets.
var (
    bktBuckets = []byte("b") // S3 bucket registry: name → bucketRecord
    bktObjects = []byte("o") // Object metadata: indexKey(bucket,key) → ObjectMeta
)

type bucketRecord struct {
    Name      string `json:"name"`
    CreatedAt string `json:"created_at"`
}

// indexKey builds the composite bbolt key for a given S3 bucket+key pair.
//
// Format: <bucket_name> 0x00 <object_key>
//
// Why null byte? S3 object keys are valid UTF-8 and can contain any printable
// character including '/', but null bytes (0x00) are not valid in S3 keys.
// This gives us a collision-free separator that also preserves lexicographic
// ordering — all objects in the same bucket sort together, and within a bucket
// they sort by key. This makes prefix-range scans via bbolt's Cursor exact
// and efficient: seek to <bucket>\x00<prefix>, iterate forward until the
// key no longer has our bucket prefix.
func indexKey(bucket, key string) []byte {
    b := make([]byte, len(bucket)+1+len(key))
    copy(b, bucket)
    b[len(bucket)] = 0x00
    copy(b[len(bucket)+1:], key)
    return b
}

// indexPrefix returns the prefix used for scanning all objects in a bucket.
// Equivalent to indexKey(bucket, "").
func indexPrefix(bucket string) []byte {
    b := make([]byte, len(bucket)+1)
    copy(b, bucket)
    b[len(bucket)] = 0x00
    return b
}

// initSchema creates all required bbolt buckets inside a write transaction.
// Called once during Open(). bbolt's CreateBucketIfNotExists is idempotent.
func initSchema(tx *bolt.Tx) error {
    for _, name := range [][]byte{bktBuckets, bktObjects} {
        if _, err := tx.CreateBucketIfNotExists(name); err != nil {
            return fmt.Errorf("store: init bbolt namespace %q: %w", name, err)
        }
    }
    return nil
}

func encodeBucketRecord(r bucketRecord) ([]byte, error) {
    data, err := json.Marshal(r)
    if err != nil {
        return nil, fmt.Errorf("store: encode bucket record: %w", err)
    }
    return data, nil
}

func encodeObjectMeta(m ObjectMeta) ([]byte, error) {
    data, err := json.Marshal(m)
    if err != nil {
        return nil, fmt.Errorf("store: encode object meta: %w", err)
    }
    return data, nil
}

func decodeObjectMeta(data []byte) (ObjectMeta, error) {
    var m ObjectMeta
    if err := json.Unmarshal(data, &m); err != nil {
        return ObjectMeta{}, fmt.Errorf("store: decode object meta: %w", err)
    }
    return m, nil
}