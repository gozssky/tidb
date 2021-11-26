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
	"io"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pkgkv "github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/restore"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/atomic"
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

type DupKVStream interface {
	Next(ctx context.Context) (key, val []byte, err error)
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
	return &LocalDupKVStream{iter: newDupDBIter(dupDB, keyAdapter, opts)}
}

func (s *LocalDupKVStream) Next(_ context.Context) (key, val []byte, err error) {
	if !s.iter.Next() {
		err = s.iter.Error()
		if err == nil {
			err = io.EOF
		}
		return
	}
	key = append(key, s.iter.Key()...)
	val = append(val, s.iter.Value()...)
	return
}

func (s *LocalDupKVStream) Close() error {
	return s.iter.Close()
}

type RemoteDupKVStream struct {
	splitCli            restore.SplitClient
	importClientFactory ImportClientFactory
	keyRange            tidbkv.KeyRange
	keyOnly             bool
	dupKVCh             chan [2][]byte
	atomicErr           atomic.Error
	cancel              context.CancelFunc
	doneCh              chan struct{}
}

func NewRemoteDupKVStream(
	splitCli restore.SplitClient,
	importClientFactory ImportClientFactory,
	keyRange tidbkv.KeyRange,
	keyOnly bool,
) (*RemoteDupKVStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &RemoteDupKVStream{
		splitCli:            splitCli,
		importClientFactory: importClientFactory,
		keyRange:            keyRange,
		keyOnly:             keyOnly,
		dupKVCh:             make(chan [2][]byte, 16),
		cancel:              cancel,
		doneCh:              make(chan struct{}),
	}
	go s.dupDetectLoop(ctx)
	return s, nil
}

func (s *RemoteDupKVStream) getDupDetectClient(
	ctx context.Context,
	region *restore.RegionInfo,
	startKey, endKey []byte,
) (import_sstpb.ImportSST_DuplicateDetectClient, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}
	importClient, err := s.importClientFactory.Create(ctx, leader.GetStoreId())
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
		StartKey: startKey,
		EndKey:   endKey,
		KeyOnly:  s.keyOnly,
	}
	return importClient.DuplicateDetect(ctx, req)
}

func (s *RemoteDupKVStream) dupDetect(ctx context.Context, region *restore.RegionInfo, startKey, endKey []byte) error {
	if bytes.Compare(startKey, region.Region.StartKey) < 0 {
		startKey = region.Region.StartKey
	}
	if len(region.Region.EndKey) > 0 && bytes.Compare(region.Region.EndKey, endKey) < 0 {
		endKey = region.Region.EndKey
	}
	cli, err := s.getDupDetectClient(ctx, region, startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		resp, err := cli.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return errors.Trace(err)
		}
		if resp.KeyError != nil {
			return errors.Errorf("meet key error in duplicate detect response: %s", resp.KeyError.Message)
		}
		for _, pair := range resp.Pairs {
			select {
			case s.dupKVCh <- [2][]byte{pair.Key, pair.Value}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (s *RemoteDupKVStream) dupDetectLoop(ctx context.Context) {
	defer close(s.doneCh)
	defer close(s.dupKVCh)
	startKey := codec.EncodeBytes([]byte{}, s.keyRange.StartKey)
	endKey := codec.EncodeBytes([]byte{}, s.keyRange.EndKey)
	for i := 0; i < maxRetryTimes; i++ {
		regions, err := restore.PaginateScanRegion(ctx, s.splitCli, startKey, endKey, scanRegionLimit)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				s.atomicErr.Store(errors.New("stream closed"))
			} else {
				s.atomicErr.Store(errors.Trace(err))
			}
			return
		}
		for _, region := range regions {
			if err := s.dupDetect(ctx, region, startKey, endKey); err != nil {
				if errors.Cause(err) == context.Canceled {
					s.atomicErr.Store(errors.New("stream closed"))
					return
				}
				log.L().Warn(
					"failed to detect duplicates",
					zap.Uint64("regionID", region.Region.GetId()),
					zap.Error(err),
				)
				// TODO: check error and retry
				break
			}
			if len(region.Region.StartKey) > 0 {
				startKey = region.Region.StartKey
			} else {
				startKey = endKey
			}
		}
		if bytes.Compare(startKey, endKey) >= 0 {
			s.atomicErr.Store(io.EOF)
			break
		}
	}
}

func (s *RemoteDupKVStream) Next(ctx context.Context) (key, val []byte, err error) {
	select {
	case kvPair, ok := <-s.dupKVCh:
		if ok {
			key = kvPair[0]
			val = kvPair[1]
		} else {
			err = s.atomicErr.Load()
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

func (s *RemoteDupKVStream) Close() error {
	s.cancel()
	<-s.doneCh
	return nil
}

type DuplicateCollector struct {
}
