package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbPath string
	engine *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{dbPath: conf.DBPath, engine: nil}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.dbPath
	opts.ValueDir = s.dbPath

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.engine = db

	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.engine == nil {
		return nil, errors.New("storage not started")
	}

	reader := NewStandAloneStorageReader(s.engine)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.engine == nil {
		return errors.New("storage not started")
	}

	txn := s.engine.NewTransaction(true)
	defer txn.Discard()

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key())); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(db *badger.DB) *StandAloneStorageReader {
	txn := db.NewTransaction(false)
	return &StandAloneStorageReader{txn}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
