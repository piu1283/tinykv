package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	reader, err := server.storage.Reader(req.Context)
	response := new(kvrpcpb.RawGetResponse)
	if err != nil {
		log.Error("Error occur when trying to get reader: ", err)
		response.Error = err.Error()
		return response, err
	}
	if val, err := reader.GetCF(cf, key); err != nil {
		log.Error("Error occur when trying to get Key: ", key)
		log.Error("Detail: ", err)
		response.Error = err.Error()
		return response, err
	} else if val == nil {
		// if the key not found
		response.Value = nil
		response.NotFound = true
	} else {
		response.Value = val
		response.NotFound = false
	}
	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	val := req.GetValue()
	modifies := make([]storage.Modify, 1)
	put := storage.Put{
		key,
		val,
		cf,
	}
	//modifies = append(modifies, storage.Modify{Data: put})
	modifies[0] = storage.Modify{Data:put}
	err := server.storage.Write(req.GetContext(), modifies)
	response := new(kvrpcpb.RawPutResponse)
	if err != nil {
		log.Error("Fail to Put CF, Detail: ", err)
		response.Error = err.Error()
		return response, err
	}
	return response, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	modifies := make([]storage.Modify, 1)
	deleteModify := storage.Delete{
		Key: key,
		Cf:  cf,
	}
	//modifies = append(modifies, storage.Modify{Data: deleteModify})
	modifies[0] = storage.Modify{Data:deleteModify}
	err := server.storage.Write(req.GetContext(), modifies)
	response := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		log.Error("Fail to Put CF, Detail: ", err)
		response.Error = err.Error()
		return response, err
	}
	return response, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	cf := req.GetCf()
	limit := req.GetLimit()
	start := req.GetStartKey()
	response := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
		return response, err
	}
	iterator := reader.IterCF(cf)
	iterator.Seek(start)
	for i := uint32(0); i < limit && iterator.Valid(); i++ {
		item := iterator.Item()
		pair := &kvrpcpb.KvPair{
			Key: item.Key(),
		}
		v, _ := item.Value()
		pair.Value = v
		response.Kvs = append(response.Kvs, pair)
		iterator.Next()
	}
	return response, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
