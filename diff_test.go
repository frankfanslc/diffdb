package diffdb

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
	"strconv"
	"github.com/hashicorp/go-multierror"
)

func NewIDObject(id []byte, o interface{}) IDObject {
	return IDObject{
		id:     id,
		Object: o,
	}
}

type IDObject struct {
	id     []byte
	Object interface{}
}

func (o IDObject) ID() []byte {
	return o.id
}

type DifferentialTestCase struct {
	With Object
	// Changed is a slightly modified version of With
	// which should generate a different hash.
	Changed Object
}

func (tc DifferentialTestCase) Run(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "_diff")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := New(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	diff, err := db.Open("test")
	if err != nil {
		t.Fatal(err)
	}

	var tracking = diff.CountTracking()
	if tracking != 0 {
		t.Fatalf("Expected nothing to be tracked; got %d", tracking)
	}
	var pending = diff.CountChanges()
	if pending != 0 {
		t.Fatalf("Expected nothing to be changed; got %d", pending)
	}

	if _, err := diff.Add(tc.With); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expected 1 item in pending changes; got %d", pending)
	}

	if _, err := diff.Add(tc.With); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expected one item to be pending changes after second call to add; got %d", pending)
	}

	err = diff.Each(context.Background(), func(id []byte, decoder Decoder) error {
		if bytes.Compare(id, tc.With.ID()) != 0 {
			return errors.Errorf("Expected ID of %x; got %x", tc.With.ID(), id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	tracking = diff.CountTracking()
	pending = diff.CountChanges()
	if tracking != 1 {
		t.Fatalf("Expected 1 item to be tracked; got %d", tracking)
	}
	if pending != 0 {
		t.Fatalf("Expected 0 items to be pending; got %d", pending)
	}

	if _, err := diff.Add(tc.Changed); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expecting 1 changed items; got %d", pending)
	}

	err = diff.Each(context.Background(), func(id []byte, decoder Decoder) error {
		if bytes.Compare(id, tc.With.ID()) != 0 {
			return errors.Errorf("Expected ID of %x; got %x", tc.With.ID(), id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	tracking = diff.CountTracking()
	pending = diff.CountChanges()
	if tracking != 1 {
		t.Fatalf("Expected 1 item to be tracked; got %d", tracking)
	}
	if pending != 0 {
		t.Fatalf("Expected 0 items to be pending; got %d", pending)
	}
}

type structTest struct {
	Key1 string
	Key2 int64
}

func TestDifferential_Add(t *testing.T) {
	var cases = []DifferentialTestCase{
		{
			With:    NewIDObject([]byte("[]byte"), []byte("data1")),
			Changed: NewIDObject([]byte("[]byte"), []byte("data2")),
		},
		{
			With:    NewIDObject([]byte("string"), "string1"),
			Changed: NewIDObject([]byte("string"), "string2"),
		},
		{
			With:    NewIDObject([]byte("int64"), int64(1)),
			Changed: NewIDObject([]byte("int64"), int64(2)),
		},
		{
			With: NewIDObject([]byte("struct"), structTest{
				Key1: "Key1",
				Key2: 1,
			}),
			Changed: NewIDObject([]byte("struct"), structTest{
				Key1: "Key2",
				Key2: 2,
			}),
		},
		{
			With: NewIDObject([]byte("map"), map[string]interface{}{
				"Key1": "Value1",
				"Key2": "Value2",
				"Key3": "Value3",
			}),
			Changed: NewIDObject([]byte("map"), map[string]interface{}{
				"Key1": "AltValue1",
				"Key2": "AltValue2",
				"Key3": "AltValue3",
			}),
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.With.ID()), tc.Run)
	}
}

type IDMapper struct {
	id []byte
}

func (id IDMapper) ID() []byte {
	return id.id
}

func TestDifferential_MustNotConflict(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "_diff")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := New(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	diff, err := db.Open("test")
	if err != nil {
		t.Fatal(err)
	}

	if err := diff.MustNotConflict(); err != nil {
		t.Fatal(err)
	}

	if _, err := diff.Add(IDMapper{id: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := diff.Add(IDMapper{id: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	_, err = diff.Add(IDMapper{id: []byte("1")})
	if err == nil {
		t.Fatal("Expected an error to be raised")
	}
	if err != ErrConflictingKey {
		t.Fatalf("Expected %q as error; got %q", ErrConflictingKey, err)
	}
}

// Test that when a context is cancelled the currently applied changes up that point are
// still committed to the database.
func TestDifferential_Each_ContextCommit(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "_diff")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := New(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	diff, err := db.Open("test_context_commit")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i ++ {
		_, err := diff.Add(NewIDObject([]byte(strconv.Itoa(i)), i))
		if err != nil {
			t.Fatal(err)
		}
	}

	if diff.CountChanges() != 10 {
		t.Fatalf("Expected 10 changes; got %d", diff.CountChanges())
	}

	var x int
	ctx, cancel := context.WithCancel(context.Background())
	err = diff.Each(ctx, func(id []byte, data Decoder) error {
		x++
		if x == 4 {
			cancel()
		}
		return nil
	})
	if x != 4 {
		t.Fatalf("expected 4 items to be processed; got %d", x)
	}

	var ok bool
	ei := err.(*multierror.Error)
	for _, e := range ei.Errors {
		if e == context.Canceled {
			ok = true
		}
	}
	if !ok {
		t.Fatal("expected context cancelled error")
	}

	pending := diff.CountChanges()
	if pending != 6 {
		t.Fatalf("Expected 6 remaining changes; got %d", pending)
	}
}

type hashBenchmark struct {
	A string
	B int
	C []string
	D time.Time
}

func BenchmarkHash(b *testing.B) {
	var bench = &hashBenchmark{
		A: "abc",
		B: 131241231,
		C: []string{"6", "1", "732", "2341", "q341", "q34e"},
		D: time.Now(),
	}

	for i := 0; i < b.N; i ++ {
		_, err := HashOf(bench)
		if err != nil {
			b.Fatal(err)
		}
	}
}
