package diffdb

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/hashstructure"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"context"
)

// A Decoder decodes serialised byte data of a diff entry into a native object.
// The object passed to Decode should be the same type added to the diff.
type Decoder interface {
	Decode(interface{}) error
}

// New creates a new hashing database using the given filename
func New(path string) (*DB, error) {
	db, err := bolt.Open(path, os.FileMode(0600), nil)
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

var (
	bucketIDHashMap  = []byte("hashmap")
	bucketSeqPending = []byte("pending")
)

type DB struct {
	db *bolt.DB
}

// New creates a new named differential.
func (db *DB) New(name string) (*Differential, error) {
	q := []byte(name)
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(q)
		if err != nil {
			return err
		}
		_, err = b.CreateBucketIfNotExists(bucketIDHashMap)
		if err != nil {
			return err
		}

		_, err = b.CreateBucketIfNotExists(bucketSeqPending)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &Differential{
		q:  q,
		db: db.db,
	}, nil
}

// Delete deletes the named differential.
func (db *DB) Delete(name string) error {
	q := []byte(name)
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(q)
	})
}

// Close closes the database file.
func (db *DB) Close() error {
	return db.db.Close()
}

// A Differential tracks changes between serialised Go objects.
type Differential struct {
	q    []byte
	db   *bolt.DB
	cols []string
}

// Add adds a new item with the given id to the differential database.
// The object at x will be hashed and the hash will be compared to the stored hash of the same ID.
// If the hash does not match, or if that ID was not previously seen then the item
// is put in a pending table to be processed later.
func (diff *Differential) Add(id []byte, x interface{}) error {
	return diff.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		bhm := b.Bucket(bucketIDHashMap)
		bsp := b.Bucket(bucketSeqPending)

		hash, err := hashstructure.Hash(x, nil)
		if err != nil {
			return err
		}

		var k = make([]byte, 8)
		binary.LittleEndian.PutUint64(k, hash)

		compare := bhm.Get(id)

		// If the existing row doesn't exist or the SHA1 hash of the row data does not match
		if compare == nil || bytes.Compare(compare, k) != 0 {
			// Append this row to list of pending changes
			raw, err := msgpack.Marshal(x)
			if err != nil {
				return err
			}

			if err := bhm.Put(id, k); err != nil {
				return err
			}

			if err := bsp.Put(id, raw); err != nil {
				return err
			}
		}

		return nil
	})
}

// Count counts the number of entries in the hash tracking table.
// In other words, this is the amount of all items tracked by the differential db.
func (diff *Differential) Count() (count int) {
	diff.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		count = b.Bucket(bucketIDHashMap).Stats().KeyN
		return nil
	})

	return
}

// PendingChanges returns the number of items in the change pending bucket.
func (diff *Differential) PendingChanges() (pending int) {
	diff.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		pending = b.Bucket(bucketSeqPending).Stats().KeyN
		return nil
	})

	return
}

var _ Decoder = (*msgpackDecoder)(nil)

// msgpackDecoder uses the msgpack library to unmarshal differential data
type msgpackDecoder struct {
	data []byte
}

func (msg *msgpackDecoder) Decode(x interface{}) error {
	r := bytes.NewReader(msg.data)
	return msgpack.NewDecoder(r).Decode(x)
}

// Each calls f for each item in the pending changes table.
// If no error is returned by f then the item is removed from the pending changes.
// If an error is returned then the item will persist in pending changes to be re-processed at a later date.
//
// Each call to f that raises an error will be appended to a multierror slice unless reading the database itself
// raises an error.
func (diff *Differential) Each(ctx context.Context, f func(Decoder) error) error {
	var fe, ue error

	ue = diff.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		bsp := b.Bucket(bucketSeqPending)

		un := new(msgpackDecoder)
		
		cur := bsp.Cursor()

		for id, data := cur.First(); id != nil; id, data = cur.Next() {
			select {
			case <-ctx.Done():
				fe = multierror.Append(fe, ctx.Err())
				return nil
			default:
			}


			un.data = data
			if err := f(un); err != nil {
				fe = multierror.Append(fe, err)
				continue
			}
			if err := bsp.Delete(id); err != nil {
				return err
			}
		}

		return nil
	})

	// Return any errors raised by BoltDB itself
	if ue != nil {
		return ue
	}

	// Return any errors raised by the f caller
	return fe
}
