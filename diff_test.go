package diffdb

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type DifferentialTestCase struct {
	ID []byte
	// With represents a type to test
	With interface{}
	// Changed is a slightly modified version of With
	// which should generate a different hash.
	Changed interface{}
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

	if err := diff.Add(tc.ID, tc.With); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expected 1 item in pending changes; got %d", pending)
	}

	if err := diff.Add(tc.ID, tc.With); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expected one item to be pending changes after second call to add; got %d", pending)
	}

	err = diff.Each(context.Background(), func(id []byte, decoder Decoder) error {
		if bytes.Compare(id, tc.ID) != 0 {
			return errors.Errorf("Expected ID of %x; got %x", tc.ID, id)
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

	if err := diff.Add(tc.ID, tc.Changed); err != nil {
		t.Fatal(err)
	}

	pending = diff.CountChanges()
	if pending != 1 {
		t.Fatalf("Expecting 1 changed items; got %d", pending)
	}

	err = diff.Each(context.Background(), func(id []byte, decoder Decoder) error {
		if bytes.Compare(id, tc.ID) != 0 {
			return errors.Errorf("Expected ID of %x; got %x", tc.ID, id)
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
			ID:      []byte("[]byte"),
			With:    []byte("data1"),
			Changed: []byte("data2"),
		},
		{
			ID:      []byte("string"),
			With:    "string1",
			Changed: "string2",
		},
		{
			ID:      []byte("int64"),
			With:    int64(1),
			Changed: int64(2),
		},
		{
			ID: []byte("struct"),
			With: structTest{
				Key1: "Key1",
				Key2: 1,
			},
			Changed: structTest{
				Key1: "Key2",
				Key2: 2,
			},
		},
		{
			ID: []byte("map"),
			With: map[string]interface{}{
				"Key1": "Value1",
				"Key2": "Value2",
				"Key3": "Value3",
			},
			Changed: map[string]interface{}{
				"Key1": "AltValue1",
				"Key2": "AltValue2",
				"Key3": "AltValue3",
			},
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.ID), tc.Run)
	}
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

	if err := diff.Add([]byte("1"), struct {}{}); err != nil {
		t.Fatal(err)
	}
	if err := diff.Add([]byte("2"), struct {}{}); err != nil {
		t.Fatal(err)
	}
	err = diff.Add([]byte("1"), struct {}{})
	if err == nil {
		t.Fatal("Expected an error to be raised")
	}
	if err != ErrConflictingKey {
		t.Fatalf("Expected %q as error; got %q", ErrConflictingKey, err)
	}
}