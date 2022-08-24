package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
	// var keys [][]byte
	// keys = append(keys, req.Key)
	// server.Latches.AcquireLatches(keys)
	// defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.GetResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.Version)
	lock, err2 := txn.GetLock(req.Key)
	if err2 != nil {
		return resp, err2
	}
	if lock != nil && lock.Ts < txn.StartTS {
		lockInfo := kvrpcpb.LockInfo{}
		lockInfo.PrimaryLock = lock.Primary
		lockInfo.Key = req.Key
		lockInfo.LockVersion = lock.Ts
		lockInfo.LockTtl = lock.Ttl
		resp.Error = &kvrpcpb.KeyError{Locked: &lockInfo}
		return resp, nil
	}
	val, err3 := txn.GetValue(req.Key)
	if err3 != nil {
		return resp, err3
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// var keys [][]byte
	// for _, mutation := range req.Mutations {
	// 	keys = append(keys, mutation.Key)
	// }
	// server.Latches.AcquireLatches(keys)
	// defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.PrewriteResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return resp, err
		}
		if write != nil && ts >= txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			return resp, nil
		}
		lock, err2 := txn.GetLock(mutation.Key)
		if err2 != nil {
			return resp, err2
		}
		if lock != nil && lock.Ts != txn.StartTS {
			lockInfo := kvrpcpb.LockInfo{}
			lockInfo.PrimaryLock = lock.Primary
			lockInfo.Key = mutation.Key
			lockInfo.LockVersion = lock.Ts
			lockInfo.LockTtl = lock.Ttl
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: &lockInfo})
			return resp, nil
		}
		var kind mvcc.WriteKind
		if mutation.Op == kvrpcpb.Op_Put {
			txn.PutValue(mutation.Key, mutation.Value)
			kind = mvcc.WriteKindPut
		} else if mutation.Op == kvrpcpb.Op_Del {
			txn.DeleteValue(mutation.Key)
			kind = mvcc.WriteKindDelete
		} else {
			return resp, nil
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}
	err2 := server.storage.Write(req.Context, txn.Writes())
	if err2 != nil {
		return nil, err2
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// server.Latches.AcquireLatches(req.Keys)
	// defer server.Latches.ReleaseLatches(req.Keys)
	resp := &kvrpcpb.CommitResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			return resp, nil
		}
		if lock != nil && lock.Ts != txn.StartTS {
			lockInfo := kvrpcpb.LockInfo{}
			lockInfo.PrimaryLock = lock.Primary
			lockInfo.Key = key
			lockInfo.LockVersion = lock.Ts
			lockInfo.LockTtl = lock.Ttl
			resp.Error = &kvrpcpb.KeyError{Locked: &lockInfo}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err2 := server.storage.Write(req.Context, txn.Writes())
	if err2 != nil {
		return nil, err2
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.LockTs)
	lockVal, err := storageReader.GetCF(engine_util.CfLock, req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lockVal != nil {
		lock, err := mvcc.ParseLock(lockVal)
		if err != nil {
			return nil, err
		}
		currentTs := mvcc.PhysicalTime(req.CurrentTs)
		startTs := mvcc.PhysicalTime(lock.Ts)
		// log.Infof("lock.Ts: %d, req.LockTs: %d", lock.Ts, req.LockTs)
		if lock.Ts == req.LockTs {
			if currentTs-startTs >= lock.Ttl {
				txn.DeleteValue(req.PrimaryKey)
				txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
					StartTS: req.LockTs,
					Kind:    mvcc.WriteKindRollback,
				})
				txn.DeleteLock(req.PrimaryKey)
				server.storage.Write(req.Context, txn.Writes())
				resp.Action = kvrpcpb.Action_TTLExpireRollback
				return resp, nil
			}
		}
		resp.Action = kvrpcpb.Action_NoAction
		resp.LockTtl = lock.Ttl
		return resp, nil
	} else {
		write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write == nil {
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			server.storage.Write(req.Context, txn.Writes())
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
		if write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		resp.Action = kvrpcpb.Action_NoAction
		resp.CommitVersion = commitTs
		return resp, nil
	}
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			continue
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		txn.DeleteValue(key)
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp1 := &kvrpcpb.ResolveLockResponse{}
	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp1.RegionError = regionErr.RequestErr
			return resp1, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, key)
		}
	}
	if req.CommitVersion > 0 {
		resp2, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys:          keys,
		})
		if err != nil {
			return nil, err
		}
		resp1.Error = resp2.Error
		resp1.RegionError = resp2.RegionError
		return resp1, nil
	} else {
		resp2, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp1.Error = resp2.Error
		resp1.RegionError = resp2.RegionError
		return resp1, nil
	}
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
