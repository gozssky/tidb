// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"context"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/sync/semaphore"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pkgkv "github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/distsql"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type pendingIndexHandles struct {
	// all 4 slices should have exactly the same length.
	// we use a struct-of-arrays instead of array-of-structs
	// so that the rawHandles can be directly given to the BatchGetRequest.
	dataConflictInfos []errormanager.DataConflictInfo
	indexNames        []string
	handles           []tidbkv.Handle
	rawHandles        [][]byte
}

// makePendingIndexHandlesWithCapacity makes the pendingIndexHandles struct-of-arrays with the given
// capacity for every internal array.
func makePendingIndexHandlesWithCapacity(cap int) pendingIndexHandles {
	return pendingIndexHandles{
		dataConflictInfos: make([]errormanager.DataConflictInfo, 0, cap),
		indexNames:        make([]string, 0, cap),
		handles:           make([]tidbkv.Handle, 0, cap),
		rawHandles:        make([][]byte, 0, cap),
	}
}

// append pushes the item (no copying) to the end of the indexHandles.
func (indexHandles *pendingIndexHandles) append(
	conflictInfo errormanager.DataConflictInfo,
	indexName string,
	handle tidbkv.Handle,
	rawHandle []byte,
) {
	indexHandles.dataConflictInfos = append(indexHandles.dataConflictInfos, conflictInfo)
	indexHandles.indexNames = append(indexHandles.indexNames, indexName)
	indexHandles.handles = append(indexHandles.handles, handle)
	indexHandles.rawHandles = append(indexHandles.rawHandles, rawHandle)
}

// appendAt pushes `other[i]` to the end of indexHandles.
func (indexHandles *pendingIndexHandles) appendAt(
	other *pendingIndexHandles,
	i int,
) {
	indexHandles.append(
		other.dataConflictInfos[i],
		other.indexNames[i],
		other.handles[i],
		other.rawHandles[i],
	)
}

// extends concatenates `other` to the end of indexHandles.
func (indexHandles *pendingIndexHandles) extend(other *pendingIndexHandles) {
	indexHandles.dataConflictInfos = append(indexHandles.dataConflictInfos, other.dataConflictInfos...)
	indexHandles.indexNames = append(indexHandles.indexNames, other.indexNames...)
	indexHandles.handles = append(indexHandles.handles, other.handles...)
	indexHandles.rawHandles = append(indexHandles.rawHandles, other.rawHandles...)
}

// truncate resets all arrays in indexHandles to length zero, but keeping the allocated capacity.
func (indexHandles *pendingIndexHandles) truncate() {
	indexHandles.dataConflictInfos = indexHandles.dataConflictInfos[:0]
	indexHandles.indexNames = indexHandles.indexNames[:0]
	indexHandles.handles = indexHandles.handles[:0]
	indexHandles.rawHandles = indexHandles.rawHandles[:0]
}

// Len implements sort.Interface.
func (indexHandles *pendingIndexHandles) Len() int {
	return len(indexHandles.rawHandles)
}

// Less implements sort.Interface.
func (indexHandles *pendingIndexHandles) Less(i, j int) bool {
	return bytes.Compare(indexHandles.rawHandles[i], indexHandles.rawHandles[j]) < 0
}

// Swap implements sort.Interface.
func (indexHandles *pendingIndexHandles) Swap(i, j int) {
	indexHandles.handles[i], indexHandles.handles[j] = indexHandles.handles[j], indexHandles.handles[i]
	indexHandles.indexNames[i], indexHandles.indexNames[j] = indexHandles.indexNames[j], indexHandles.indexNames[i]
	indexHandles.dataConflictInfos[i], indexHandles.dataConflictInfos[j] = indexHandles.dataConflictInfos[j], indexHandles.dataConflictInfos[i]
	indexHandles.rawHandles[i], indexHandles.rawHandles[j] = indexHandles.rawHandles[j], indexHandles.rawHandles[i]
}

// searchSortedRawHandle looks up for the index i such that `rawHandles[i] == rawHandle`.
// This function assumes indexHandles is already sorted, and rawHandle does exist in it.
func (indexHandles *pendingIndexHandles) searchSortedRawHandle(rawHandle []byte) int {
	return sort.Search(indexHandles.Len(), func(i int) bool {
		return bytes.Compare(indexHandles.rawHandles[i], rawHandle) >= 0
	})
}

type pendingKeyRange tidbkv.KeyRange

func (kr pendingKeyRange) Less(other btree.Item) bool {
	return bytes.Compare(kr.EndKey, other.(pendingKeyRange).EndKey) < 0
}

type pendingKeyRanges struct {
	mu   sync.Mutex
	tree *btree.BTree
}

func buildTree(keyRanges []tidbkv.KeyRange) *btree.BTree {
	tree := btree.New(32)
	if len(keyRanges) == 0 {
		return tree
	}
	sort.Slice(keyRanges, func(i, j int) bool {
		return bytes.Compare(keyRanges[i].StartKey, keyRanges[j].StartKey) < 0
	})
	startKey := keyRanges[0].StartKey
	endKey := keyRanges[0].EndKey
	for _, kr := range keyRanges[1:] {
		if bytes.Compare(kr.StartKey, endKey) > 0 {
			tree.ReplaceOrInsert(
				pendingKeyRange(tidbkv.KeyRange{
					StartKey: startKey,
					EndKey:   endKey,
				}),
			)
			startKey = kr.StartKey
			endKey = kr.EndKey
		} else if bytes.Compare(kr.EndKey, endKey) > 0 {
			endKey = kr.EndKey
		}
	}
	tree.ReplaceOrInsert(
		pendingKeyRange(tidbkv.KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		}),
	)
	return tree
}

func newPendingKeyRanges(keyRanges ...tidbkv.KeyRange) *pendingKeyRanges {
	return &pendingKeyRanges{tree: buildTree(keyRanges)}
}

func (p *pendingKeyRanges) list() []tidbkv.KeyRange {
	p.mu.Lock()
	defer p.mu.Unlock()

	var keyRanges []tidbkv.KeyRange
	p.tree.Ascend(func(item btree.Item) bool {
		keyRanges = append(keyRanges, tidbkv.KeyRange(item.(pendingKeyRange)))
		return true
	})
	return keyRanges
}

func (p *pendingKeyRanges) empty() bool {
	return p.tree.Min() == nil
}

func (p *pendingKeyRanges) finish(keyRange tidbkv.KeyRange) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		pendingAdd    []btree.Item
		pendingRemove []btree.Item
	)
	startKey := keyRange.StartKey
	endKey := keyRange.EndKey
	p.tree.AscendGreaterOrEqual(
		pendingKeyRange(tidbkv.KeyRange{EndKey: startKey}),
		func(item btree.Item) bool {
			kr := item.(pendingKeyRange)
			if bytes.Compare(startKey, kr.EndKey) >= 0 {
				return true
			}
			if bytes.Compare(endKey, kr.StartKey) <= 0 {
				return false
			}
			pendingRemove = append(pendingRemove, kr)
			if bytes.Compare(startKey, kr.StartKey) > 0 {
				pendingAdd = append(pendingAdd,
					pendingKeyRange(tidbkv.KeyRange{
						StartKey: kr.StartKey,
						EndKey:   startKey,
					}),
				)
			}
			if bytes.Compare(endKey, kr.EndKey) < 0 {
				pendingAdd = append(pendingAdd,
					pendingKeyRange(tidbkv.KeyRange{
						StartKey: endKey,
						EndKey:   kr.EndKey,
					}),
				)
			}
			return true
		},
	)
	for _, item := range pendingRemove {
		p.tree.Delete(item)
	}
	for _, item := range pendingAdd {
		p.tree.ReplaceOrInsert(item)
	}
}

// physicalTableIDs returns all physical table IDs associated with the tableInfo.
// A partitioned table can have multiple physical table IDs.
func physicalTableIDs(tableInfo *model.TableInfo) []int64 {
	if tableInfo.Partition != nil {
		defs := tableInfo.Partition.Definitions
		tids := make([]int64, 1, len(defs)+1)
		tids[0] = tableInfo.ID
		for _, def := range defs {
			tids = append(tids, def.ID)
		}
		return tids
	}
	return []int64{tableInfo.ID}
}

// tableHandleKeyRanges returns all key ranges associated with the tableInfo.
func tableHandleKeyRanges(tableInfo *model.TableInfo) ([]tidbkv.KeyRange, error) {
	ranges := ranger.FullIntRange(false)
	if tableInfo.IsCommonHandle {
		ranges = ranger.FullRange()
	}
	tableIDs := physicalTableIDs(tableInfo)
	return distsql.TableHandleRangesToKVRanges(nil, tableIDs, tableInfo.IsCommonHandle, ranges, nil)
}

// tableIndexKeyRanges returns all key ranges associated with the tableInfo and indexInfo.
func tableIndexKeyRanges(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) ([]tidbkv.KeyRange, error) {
	tableIDs := physicalTableIDs(tableInfo)
	var keyRanges []tidbkv.KeyRange
	for _, tid := range tableIDs {
		partitionKeysRanges, err := distsql.IndexRangesToKVRanges(nil, tid, indexInfo.ID, ranger.FullRange(), nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		keyRanges = append(keyRanges, partitionKeysRanges...)
	}
	return keyRanges, nil
}

type DupKVStream interface {
	Next() (key, val []byte, err error)
	Close() error
}

//goland:noinspection GoNameStartsWithPackageName
type LocalDupKVStream struct {
	iter pkgkv.Iter
}

func NewLocalDupKVStream(dupDB *pebble.DB, keyAdapter KeyAdapter, keyRange tidbkv.KeyRange) *LocalDupKVStream {
	opts := &pebble.IterOptions{
		LowerBound: keyRange.StartKey,
		UpperBound: keyRange.EndKey,
	}
	iter := newDupDBIter(dupDB, keyAdapter, opts)
	iter.First()
	return &LocalDupKVStream{iter: iter}
}

func (s *LocalDupKVStream) Next() (key, val []byte, err error) {
	if !s.iter.Valid() {
		err = s.iter.Error()
		if err == nil {
			err = io.EOF
		}
		return
	}
	key = append(key, s.iter.Key()...)
	val = append(val, s.iter.Value()...)
	s.iter.Next()
	return
}

func (s *LocalDupKVStream) Close() error {
	return s.iter.Close()
}

type RemoteDupKVStream struct {
	cli    import_sstpb.ImportSST_DuplicateDetectClient
	kvs    []*import_sstpb.KvPair
	atEOF  bool
	cancel context.CancelFunc
}

func getDupDetectClient(
	ctx context.Context,
	region *restore.RegionInfo,
	keyRange tidbkv.KeyRange,
	importClientFactory ImportClientFactory,
) (import_sstpb.ImportSST_DuplicateDetectClient, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}
	importClient, err := importClientFactory.Create(ctx, leader.GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}
	req := &import_sstpb.DuplicateDetectRequest{
		Context:  reqCtx,
		StartKey: keyRange.StartKey,
		EndKey:   keyRange.EndKey,
	}
	return importClient.DuplicateDetect(ctx, req)
}

func NewRemoteDupKVStream(
	region *restore.RegionInfo,
	keyRange tidbkv.KeyRange,
	importClientFactory ImportClientFactory,
) (*RemoteDupKVStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cli, err := getDupDetectClient(ctx, region, keyRange, importClientFactory)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	return &RemoteDupKVStream{cli: cli, cancel: cancel}, nil
}

func (s *RemoteDupKVStream) tryRecv() error {
	resp, err := s.cli.Recv()
	if err != nil {
		if errors.Cause(err) == io.EOF {
			s.atEOF = true
			err = io.EOF
		}
		return err
	}
	if resp.KeyError != nil {
		return errors.Errorf("meet key error in duplicate detect response: %s", resp.KeyError.Message)
	}
	s.kvs = resp.Pairs
	return nil
}

func (s *RemoteDupKVStream) Next() (key, val []byte, err error) {
	if len(s.kvs) == 0 {
		if s.atEOF {
			return nil, nil, io.EOF
		}
		if err := s.tryRecv(); err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	key, val = s.kvs[0].Key, s.kvs[0].Value
	s.kvs = s.kvs[1:]
	return
}

func (s *RemoteDupKVStream) Close() error {
	s.cancel()
	return nil
}

type DuplicateManager struct {
	tbl       table.Table
	tableName string
	splitCli  restore.SplitClient
	tikvCli   *tikv.KVStore
	errMgr    *errormanager.ErrorManager
	decoder   *kv.TableKVDecoder
	logger    log.Logger

	regionConcurrency int
}

func NewDuplicateManager(
	tbl table.Table,
	tableName string,
	splitCli restore.SplitClient,
	tikvCli *tikv.KVStore,
	errMgr *errormanager.ErrorManager,
	sessOpts *kv.SessionOptions,
) (*DuplicateManager, error) {
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, sessOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger := log.With(zap.String("tableName", tableName))
	return &DuplicateManager{
		tbl:       tbl,
		tableName: tableName,
		splitCli:  splitCli,
		tikvCli:   tikvCli,
		errMgr:    errMgr,
		decoder:   decoder,
		logger:    logger,
	}, nil
}

func (m *DuplicateManager) RecordDataConflictError(ctx context.Context, stream DupKVStream) error {
	defer stream.Close()
	var dataConflictInfos []errormanager.DataConflictInfo
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		h, err := m.decoder.DecodeHandleFromRowKey(key)
		if err != nil {
			return errors.Trace(err)
		}
		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   key,
			RawValue: val,
			KeyData:  h.String(),
			Row:      m.decoder.DecodeRawRowDataAsStr(h, val),
		}
		dataConflictInfos = append(dataConflictInfos, conflictInfo)
		if len(dataConflictInfos) >= 1024 {
			if err := m.errMgr.RecordDataConflictError(ctx, m.logger, m.tableName, dataConflictInfos); err != nil {
				return errors.Trace(err)
			}
			dataConflictInfos = dataConflictInfos[:0]
		}
	}
	err := m.errMgr.RecordDataConflictError(ctx, m.logger, m.tableName, dataConflictInfos)
	return errors.Trace(err)
}

func (m *DuplicateManager) saveIndexHandles(ctx context.Context, handles pendingIndexHandles) error {
	snapshot := m.tikvCli.GetSnapshot(math.MaxUint64)
	batchGetMap, err := snapshot.BatchGet(ctx, handles.rawHandles)
	if err != nil {
		return errors.Trace(err)
	}
	rawRows := make([][]byte, 0, handles.Len())
	for i, rawHandle := range handles.rawHandles {
		rawValue, ok := batchGetMap[string(hack.String(rawHandle))]
		if ok {
			rawRows = append(rawRows, rawValue)
			handles.dataConflictInfos[i].Row = m.decoder.DecodeRawRowDataAsStr(handles.handles[i], rawValue)
		} else {
			// TODO: add warn.
		}
	}
	err = m.errMgr.RecordIndexConflictError(ctx, m.logger, m.tableName,
		handles.indexNames, handles.dataConflictInfos, handles.rawHandles, rawRows)
	return errors.Trace(err)
}

func (m *DuplicateManager) RecordIndexConflictError(ctx context.Context, stream DupKVStream, tableID int64, indexInfo *model.IndexInfo) error {
	defer stream.Close()
	indexHandles := makePendingIndexHandlesWithCapacity(0)
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		h, err := m.decoder.DecodeHandleFromIndex(indexInfo, key, val)
		if err != nil {
			return errors.Trace(err)
		}
		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   key,
			RawValue: val,
			KeyData:  h.String(),
		}
		indexHandles.append(conflictInfo, indexInfo.Name.O,
			h, tablecodec.EncodeRowKeyWithHandle(tableID, h))

		if indexHandles.Len() >= 1024 {
			if err := m.saveIndexHandles(ctx, indexHandles); err != nil {
				return errors.Trace(err)
			}
			indexHandles.truncate()
		}
	}
	err := m.saveIndexHandles(ctx, indexHandles)
	return errors.Trace(err)
}

func (m *DuplicateManager) CollectDuplicateRowsFromDupDB(ctx context.Context, dupDB *pebble.DB, keyAdapter KeyAdapter) error {
	handleKeyRanges, err := tableHandleKeyRanges(m.tbl.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	for _, keyRange := range handleKeyRanges {
		stream := NewLocalDupKVStream(dupDB, keyAdapter, keyRange)
		if err := m.RecordDataConflictError(ctx, stream); err != nil {
			return errors.Trace(err)
		}
	}
	for _, indexInfo := range m.tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		keyRanges, err := tableIndexKeyRanges(m.tbl.Meta(), indexInfo)
		if err != nil {
			return errors.Trace(err)
		}
		for _, keyRange := range keyRanges {
			tableID := tablecodec.DecodeTableID(keyRange.StartKey)
			stream := NewLocalDupKVStream(dupDB, keyAdapter, keyRange)
			if err := m.RecordIndexConflictError(ctx, stream, tableID, indexInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

type remoteDupTask struct {
	tidbkv.KeyRange
	tableID   int64
	indexInfo *model.IndexInfo
}

// processRemoteDupTask collects remote duplicates within the given key range.
// A key range is associated with multiple regions. collectRemoteDupByRange tries
// to collect duplicates from each region. If the duplicates for a region are not
// successfully collected, the corresponding key range will be sent to retryKeyRangeCh.
func (m *DuplicateManager) processRemoteDupTask(
	ctx context.Context,
	task remoteDupTask,
	importClientFactory ImportClientFactory,
	taskCh chan<- remoteDupTask,
	activeTasks *sync.WaitGroup,
	concurrencyLimit *semaphore.Weighted,
) error {
	rawStartKey := codec.EncodeBytes(nil, task.StartKey)
	rawEndKey := codec.EncodeBytes(nil, task.EndKey)
	regions, err := restore.PaginateScanRegion(ctx, m.splitCli, rawStartKey, rawEndKey, 1024)
	if err != nil {
		return errors.Trace(err)
	}
	wg := &sync.WaitGroup{}
	for _, region1 := range regions {
		region := region1
		// TODO: check error
		_, startKey, _ := codec.DecodeBytes(nil, region.Region.StartKey)
		_, endKey, _ := codec.DecodeBytes(nil, region.Region.EndKey)
		if bytes.Compare(startKey, task.StartKey) < 0 {
			startKey = task.StartKey
		}
		if bytes.Compare(endKey, task.EndKey) > 0 {
			endKey = task.EndKey
		}
		if bytes.Compare(startKey, endKey) >= 0 {
			continue
		}

		wg.Add(1)
		concurrencyLimit.Acquire(ctx, 1)
		// TODO: comment.
		activeTasks.Add(1)
		go func() {
			defer wg.Done()
			defer concurrencyLimit.Release(1)

			var err error
			stream, err := NewRemoteDupKVStream(region, tidbkv.KeyRange{
				StartKey: startKey,
				EndKey:   endKey,
			}, importClientFactory)
			if err != nil {
				// TODO: log
				return
			}
			if task.indexInfo == nil {
				m.RecordDataConflictError(ctx, stream)
			} else {
				m.RecordIndexConflictError(ctx, stream, task.tableID, task.indexInfo)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (m *DuplicateManager) CollectDuplicateRowsFromTiKV(ctx context.Context, importClientFactory ImportClientFactory) error {
	var tasks []remoteDupTask
	keyRanges, err := tableHandleKeyRanges(m.tbl.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	for _, kr := range keyRanges {
		tableID := tablecodec.DecodeTableID(kr.StartKey)
		tasks = append(tasks, remoteDupTask{
			KeyRange: kr,
			tableID:  tableID,
		})
	}
	for _, indexInfo := range m.tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		keyRanges, err = tableIndexKeyRanges(m.tbl.Meta(), indexInfo)
		if err != nil {
			return errors.Trace(err)
		}
		for _, kr := range keyRanges {
			tableID := tablecodec.DecodeTableID(kr.StartKey)
			tasks = append(tasks, remoteDupTask{
				KeyRange:  kr,
				tableID:   tableID,
				indexInfo: indexInfo,
			})
		}
	}

	taskCh := make(chan remoteDupTask, len(tasks))
	for _, task := range tasks {
		taskCh <- task
	}
	regionLimit := semaphore.NewWeighted(int64(m.regionConcurrency))

	wg := &sync.WaitGroup{}
	activeTasks := &sync.WaitGroup{}
	activeTasks.Add(len(tasks))
	for i := 0; i < m.regionConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				m.processRemoteDupTask(ctx, task, importClientFactory, taskCh, activeTasks, regionLimit)
				activeTasks.Done()
			}
		}()
	}
	activeTasks.Wait()
	close(taskCh)
	wg.Wait()

	return nil
}
