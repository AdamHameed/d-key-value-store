package storage

import (
	"errors"
	"testing"
)

func TestStorePutGetDelete(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Put("alpha", []byte("one")); err != nil {
		t.Fatal(err)
	}
	value, err := store.Get("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "one" {
		t.Fatalf("got %q, want one", value)
	}
	if err := store.Delete("alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get("alpha"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got err %v, want ErrNotFound", err)
	}
}

func TestBadgerWALRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("survives", []byte("restart")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, err := reopened.Get("survives")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "restart" {
		t.Fatalf("got %q, want restart", value)
	}
}

func TestSnapshotRestore(t *testing.T) {
	source, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := source.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	snap, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	target, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.Put("stale", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := target.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Get("stale"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale key after restore: %v", err)
	}
	value, err := target.Get("b")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "2" {
		t.Fatalf("got %q, want 2", value)
	}
}
