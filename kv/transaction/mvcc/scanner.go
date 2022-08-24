package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scan := &Scanner{
		txn:  txn,
		iter: iter,
	}
	encodeKey := EncodeKey(startKey, txn.StartTS)
	scan.iter.Seek(encodeKey)
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	encodeKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(encodeKey)
	commitTs := decodeTimestamp(encodeKey)
	// 防止定位到了本该定位的key的下一个key
	if commitTs > scan.txn.StartTS {
		scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
		return scan.Next()
	}
	lock, err := scan.txn.GetLock(userKey)
	if err != nil {
		return nil, nil, err
	}
	if lock == nil || lock.Ts >= scan.txn.StartTS {
		writeVal, err := item.ValueCopy(nil)
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(writeVal)
		if err != nil {
			return nil, nil, err
		}
		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
		if err != nil {
			return nil, nil, err
		}
		if write.Kind == WriteKindRollback {
			scan.iter.Next()
			return scan.Next()
		}
		if write.Kind != WriteKindDelete {
			// 返回之前，先找到下一个key
			scan.iter.Seek(EncodeKey(userKey, 0))
			for scan.iter.Valid() {
				nextItem := scan.iter.Item()
				nextUserKey := DecodeUserKey(nextItem.KeyCopy(nil))
				if !bytes.Equal(nextUserKey, userKey) {
					scan.iter.Seek(EncodeKey(nextUserKey, scan.txn.StartTS))
					break
				}
				scan.iter.Next()
			}
			return userKey, value, nil
		}
	}
	// 当前key被上锁或者被标记删除
	// 寻找下一个key
	scan.iter.Seek(EncodeKey(userKey, 0))
	for scan.iter.Valid() {
		nextItem := scan.iter.Item()
		nextUserKey := DecodeUserKey(nextItem.KeyCopy(nil))
		if !bytes.Equal(nextUserKey, userKey) {
			scan.iter.Seek(EncodeKey(nextUserKey, scan.txn.StartTS))
			break
		}
		scan.iter.Next()
	}
	return scan.Next()
}
