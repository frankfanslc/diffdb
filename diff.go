package diffdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/hashstructure"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"errors"
)

var (
	ErrConflictingKey = errors.New("diffdb: multiple objects with the same ID were added in the same change version")
)

func HashOf(x interface{}) ([]byte, error) {
	// Generate the hash using hashstructure
	huint64, err := hashstructure.Hash(x, nil)
	if err != nil {
		return nil, err
	}
	var hash = make([]byte, 8)
	binary.LittleEndian.PutUint64(hash, huint64)
	return hash, nil
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
	bucketHashes          = []byte("_m")
	bucketPendingHashes   = []byte("_ph")
	bucketPendingHashData = []byte("_pd")
	bucketUserData        = []byte("_ud")
	bucketKeyConflicts    = []byte("_dk")
)

type DB struct {
	db *bolt.DB
}

// Open opens a named differential or creates one if it does not exist.
func (db *DB) Open(name string) (*Differential, error) {
	q := []byte(name)
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(q)
		if err != nil {
			return err
		}

		_, err = b.CreateBucketIfNotExists(bucketHashes)
		if err != nil {
			return err
		}
		_, err = b.CreateBucketIfNotExists(bucketPendingHashes)
		if err != nil {
			return err
		}
		_, err = b.CreateBucketIfNotExists(bucketPendingHashData)
		if err != nil {
			return err
		}
		_, err = b.CreateBucketIfNotExists(bucketUserData)
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

	trackConflicts bool
}

// MustNotConflict sets a flag to track duplicate IDs given to subsequent calls to Add.
// This can be used as a debugging tool to check if additions in the same version
// have conflicting IDs.
// Calling MustNotConflict will delete any existing conflict information.
func (diff *Differential) MustNotConflict() error {
	return diff.db.Update(func(tx *bolt.Tx) error {
		tx.OnCommit(func(){
			diff.trackConflicts = true
		})

		b := tx.Bucket(diff.q)
		cb := b.Bucket(bucketKeyConflicts)
		if cb != nil {
			err := b.DeleteBucket(bucketKeyConflicts)
			if err != nil {
				return err
			}
		}

		_, err := b.CreateBucket(bucketKeyConflicts)
		return err
	})
}

// Add as a new object x to the list of pending changes.
// Changes to x are tracked through its given ID which uniquely identifies x across changes.
// For example, if x was an SQL row then ID would be the primary key of that row.
//
// If Add is called multiple times same ID before applying changes then
// only the latest change will be taken to be applied.
func (diff *Differential) Add(id []byte, x interface{}) error {
	return diff.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)

		var (
			bh   = b.Bucket(bucketHashes)
			bph  = b.Bucket(bucketPendingHashes)
			bphd = b.Bucket(bucketPendingHashData)
		)

		// Check ID conflicts
		if diff.trackConflicts {
			bkc := b.Bucket(bucketKeyConflicts)
			if bkc.Get(id) != nil {
				return ErrConflictingKey
			}
		}

		hash, err := HashOf(x)
		if err != nil {
			return err
		}

		var (
			existing = bh.Get(id)
			match    = bytes.Compare(existing, hash) == 0
		)

		// An existing committed hash is identical, no need for changes
		if match {
			return nil
		}

		// Check if pending hash already exists
		if pending := bph.Get(id); pending != nil {

			// Contents are identical to existing pending version, no need for changes
			if len(pending) > 0 && bytes.Compare(pending, hash) == 0 {
				return nil
			}

			if err := bphd.Delete(pending); err != nil {
				return err
			}
		}

		// Ensure this ID is ready to be tracked
		if err := bph.Put(id, hash); err != nil {
			return err
		}

		raw, err := msgpack.Marshal(x)
		if err != nil {
			return err
		}
		if err := bphd.Put(hash, raw); err != nil {
			return err
		}

		if diff.trackConflicts {
			err := b.Bucket(bucketKeyConflicts).Put(id, nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Changed returns true if the hash of x has changed for its ID.
func (diff *Differential) Changed(id []byte, x interface{}) (changed bool, err error) {
	var hash []byte
	hash, err = HashOf(x)
	if err != nil {
		return
	}

	err = diff.db.View(func(tx *bolt.Tx) error {
		var compare = tx.Bucket(diff.q).Bucket(bucketHashes).Get(id)
		changed = bytes.Compare(compare, hash) != 0
		return nil
	})
	return
}

// CountTracking counts the number of entries in the hash tracking table.
// In other words, this is the amount of all items tracked by the differential db.
func (diff *Differential) CountTracking() (count int) {
	diff.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		count = b.Bucket(bucketHashes).Stats().KeyN
		return nil
	})

	return
}

// CountChanges returns the number of items in the change pending bucket.
func (diff *Differential) CountChanges() (pending int) {
	diff.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q)
		pending = b.Bucket(bucketPendingHashes).Stats().KeyN
		return nil
	})

	return
}

// ApplyFunc is a function to be called to apply each pending change
type ApplyFunc func(id []byte, data Decoder) error

// Each scans through each change and attempts to
func (diff *Differential) Each(ctx context.Context, f ApplyFunc) error {
	tx, err := diff.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket(diff.q)
	var (
		bh   = b.Bucket(bucketHashes)
		bph  = b.Bucket(bucketPendingHashes)
		bphd = b.Bucket(bucketPendingHashData)

		decoder = new(msgpackDecoder)
		cur     = bph.Cursor()
	)

	var updateErr *multierror.Error

scan:
	for id, hash := cur.First(); id != nil; id, hash = cur.Next() {
		select {
		case <-ctx.Done():
			updateErr = multierror.Append(updateErr, ctx.Err())
			break scan
		default:
		}

		var data = bphd.Get(hash)
		if data == nil {
			panic("missing hash data")
		}

		decoder.data = data
		if err := f(id, decoder); err != nil {
			updateErr = multierror.Append(updateErr, err)
			continue
		}

		if err := bh.Put(id, hash); err != nil {
			return err
		}
		if err := bph.Delete(id); err != nil {
			return err
		}
		if err := bphd.Delete(hash); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return updateErr.ErrorOrNil()
}

// ViewUserData wraps a BoltDB view transaction to allow custom user data to be viewed in the differential database.
// This could include information such as run times, last exported differential, etc.
func (diff *Differential) ViewUserData(f func(b *bolt.Bucket) error) error {
	return diff.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q).Bucket(bucketUserData)
		return f(b)
	})
}

// UpdateUserData wraps a BoltDB update transaction to allow custom user data to viewed or updated
// in the differential database.
func (diff *Differential) UpdateUserData(f func(b *bolt.Bucket) error) error {
	return diff.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diff.q).Bucket(bucketUserData)
		return f(b)
	})
}
