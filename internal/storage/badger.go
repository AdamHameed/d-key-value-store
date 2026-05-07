package storage

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var ErrNotFound = errors.New("key not found")

type Store struct {
	db *badger.DB
}

func Open(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Put(key string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (s *Store) Get(key string) ([]byte, error) {
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			out = append([]byte(nil), value...)
			return nil
		})
	})
	return out, err
}

func (s *Store) Delete(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (s *Store) Snapshot() (map[string][]byte, error) {
	items := make(map[string][]byte)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))
			if err := item.Value(func(value []byte) error {
				items[key] = append([]byte(nil), value...)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return items, err
}

func (s *Store) Restore(items map[string][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		var keys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, key := range keys {
			if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		for key, value := range items {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
}

func EncodeSnapshot(items map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(items)
	return buf.Bytes(), err
}

func DecodeSnapshot(data []byte) (map[string][]byte, error) {
	var items map[string][]byte
	err := json.Unmarshal(data, &items)
	return items, err
}

func (s *Store) Close() error {
	return s.db.Close()
}
