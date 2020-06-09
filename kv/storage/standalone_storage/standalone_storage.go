package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	opts *badger.Options
	DB   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	// use default option and the path in conf
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	return &StandAloneStorage{&opts, nil}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("Start StandAloneStorage...")
	// open badger
	if db, err := badger.Open(*s.opts); err != nil {
		log.Fatal("Fail to start StandAloneStorage. Detail: ", err)
		return err
	} else {
		s.DB = db
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// close badger db
	log.Info("Stopping StandAloneStorage...")
	if err := s.DB.Close(); err != nil {
		log.Fatal("Fail to stop StandAloneStorage. Detail: ", err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.DB.NewTransaction(false)
	sasReader := &StandAloneReader{txn}
	return sasReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if len(batch) == 1 {
		// if only has one modification
		err := s.executeSingleModification(&batch[0])
		if err != nil {
			return err
		}
	} else {
		writeBatch := new(engine_util.WriteBatch)
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				writeBatch.SetCF(modify.Cf(), modify.Key(), modify.Value())
			case storage.Delete:
				writeBatch.DeleteCF(modify.Cf(), modify.Key())
			default:
				// it may never reach this line
				log.Error("Wrong modification type: ", modify.Data)
			}
		}
		if writeBatch.Len() > 0 {
			err := writeBatch.WriteToDB(s.DB)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *StandAloneStorage) executeSingleModification(modify *storage.Modify) error {
	switch modify.Data.(type) {
	case storage.Put:
		if err := engine_util.PutCF(s.DB, modify.Cf(), modify.Key(), modify.Value()); err != nil {
			return err
		}
		log.Debug("Finish [put] modification: ", modify.Key())
	case storage.Delete:
		if err := engine_util.DeleteCF(s.DB, modify.Cf(), modify.Key()); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		log.Debug("Finish [delete] modification: ", modify.Key())
	default:
		log.Error("Wrong modification type: ", modify.Data)
	}
	return nil
}

type StandAloneReader struct {
	// one reader contains a transaction to ensure
	// all reads via this reader are consistent
	txn *badger.Txn
}

func (sar *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sar.txn, cf, key)
	// if the key is not exist, return (val: nil, err: nil)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (sar *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sar.txn)
}

func (sar *StandAloneReader) Close() {
	// close transaction
	sar.txn.Discard()
}
