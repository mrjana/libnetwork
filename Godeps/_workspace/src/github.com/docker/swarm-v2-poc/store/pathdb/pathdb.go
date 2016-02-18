package pathdb

import (
	"fmt"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	raftstore "github.com/docker/swarm-v2-poc/store"
)

// Mock store. Mocks all Store functions using testify.Mock
type pathDb struct {
	store *raftstore.Store
}

// Register registers boltdb to libkv
func Register() {
	libkv.AddStore(store.PATHDB, New)
}

// New creates a pathdb store
func New(address []string, options *store.Config) (store.Store, error) {
	rs := options.Handle.(*raftstore.Store)
	return &pathDb{store: rs}, nil
}

// Put pathdb
func (p *pathDb) Put(key string, value []byte, opts *store.WriteOptions) error {
	return p.store.UpdateKV(key, string(value))
}

// Get pathbb
func (p *pathDb) Get(key string) (*store.KVPair, error) {
	val, err := p.store.KeyLookup(key)
	if err != nil {
		return nil, err
	}

	return &store.KVPair{
		Key:   key,
		Value: []byte(val),
	}, nil
}

// Delete pathdb
func (p *pathDb) Delete(key string) error {
	return p.store.DeleteKey(key)
}

// Exists pathdb
func (p *pathDb) Exists(key string) (bool, error) {
	_, err := p.store.KeyLookup(key)
	if err != nil {
		return false, err
	}

	return true, nil
}

// Watch pathdb
func (p *pathDb) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, fmt.Errorf("not implemented")
}

// WatchTree pathdb
func (p *pathDb) WatchTree(prefix string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, fmt.Errorf("not implemented")
}

// NewLock pathdb
func (p *pathDb) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, fmt.Errorf("not implemented")
}

// List pathdb
func (p *pathDb) List(prefix string) ([]*store.KVPair, error) {
	kvPairMap := p.store.KeyList(prefix)

	kvPairList := make([]*store.KVPair, 0, len(kvPairMap))
	for k, v := range kvPairMap {
		kvp := &store.KVPair{
			Key:   k,
			Value: []byte(v),
		}

		kvPairList = append(kvPairList, kvp)
	}

	return kvPairList, nil
}

// DeleteTree pathdb
func (p *pathDb) DeleteTree(prefix string) error {
	return fmt.Errorf("not implemented")
}

// AtomicPut pathdb
func (p *pathDb) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	if err := p.Put(key, value, nil); err != nil {
		return false, nil, err
	}

	kvp := &store.KVPair{
		Key:   key,
		Value: value,
	}

	return true, kvp, nil
}

// AtomicDelete pathdb
func (p *pathDb) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if err := p.Delete(key); err != nil {
		return false, err
	}

	return true, nil
}

// Lock pathdb implementation of Locker
type Lock struct {
}

// Lock pathdb
func (l *Lock) Lock(stopCh chan struct{}) (<-chan struct{}, error) {
	return nil, fmt.Errorf("not implemened")
}

// Unlock pathdb
func (l *Lock) Unlock() error {
	return fmt.Errorf("not implemented")
}

// Close pathDb
func (p *pathDb) Close() {
	return
}
