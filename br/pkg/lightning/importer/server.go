// Copyright 2022 PingCAP, Inc.
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

package importer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/importer/kv"
	"github.com/pingcap/tidb/util/arena"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	defaultServerAddr = ":8287"
	defaultPDAddr     = "127.0.0.1:2379"
	defaultDataDir    = "/tmp/importer"

	maxGRPCMsgSize        = 128 << 20
	maxPendingSSTFileSize = 128 << 20

	queryParamStartKey = "startKey"
	queryParamEndKey   = "endKey"
)

type serverOptions struct {
	addr    string
	pdAddr  string
	dataDir string
}

var defaultServerOptions = serverOptions{
	addr:    defaultServerAddr,
	pdAddr:  defaultPDAddr,
	dataDir: defaultDataDir,
}

type funcServerOption struct {
	f func(*serverOptions)
}

func (fdo *funcServerOption) apply(do *serverOptions) {
	fdo.f(do)
}

func newFuncServerOption(f func(*serverOptions)) *funcServerOption {
	return &funcServerOption{
		f: f,
	}
}

type ServerOption interface {
	apply(*serverOptions)
}

func WithServerAddr(addr string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.addr = addr
	})
}

func WithPDAddr(pdAddr string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.pdAddr = pdAddr
	})
}

func WithDataDir(dataDir string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.dataDir = dataDir
	})
}

type Server struct {
	opts serverOptions

	kvStore *tikv.KVStore

	idGen       atomic.Int64
	compacted   atomic.Bool
	compactOnce sync.Once
	pendingSSTs struct {
		sync.Mutex
		ssts []*kv.SSTMeta
	}
	sortedSSTs []*kv.SSTMeta
}

func NewServer(opts ...ServerOption) (*Server, error) {
	options := defaultServerOptions
	for _, opt := range opts {
		opt.apply(&options)
	}
	if err := os.MkdirAll(options.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", options.dataDir, err)
	}

	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxGRPCMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxGRPCMsgSize)),
	}
	pdCli, err := pd.NewClientWithContext(
		context.Background(), []string{options.pdAddr}, pd.SecurityOption{},
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		pd.WithCustomTimeoutOption(60*time.Second),
		pd.WithMaxErrorRetry(3),
	)
	if err != nil {
		return nil, fmt.Errorf("create pd client: %w", err)
	}
	pdClient := tikv.CodecPDClient{Client: pdCli}
	spkv, err := tikv.NewEtcdSafePointKV([]string{options.pdAddr}, nil)
	if err != nil {
		pdClient.Close()
		return nil, fmt.Errorf("create safe point kv: %w", err)
	}
	kvStore, err := tikv.NewKVStore("tikv-importer", &pdClient, spkv, tikv.NewRPCClient())
	if err != nil {
		pdClient.Close()
		_ = spkv.Close()
		return nil, fmt.Errorf("create kv store: %w", err)
	}

	return &Server{
		opts:    options,
		kvStore: kvStore,
	}, nil
}

func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/read", s.handleRead)
	mux.HandleFunc("/write", s.handleWrite)
	mux.HandleFunc("/compact", s.handleCompact)
	mux.HandleFunc("/import", s.handleImport)

	httpServer := http.Server{
		Addr:    s.opts.addr,
		Handler: mux,
	}
	return httpServer.ListenAndServe()
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	if !s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has not been compacted"))
		return
	}

	startKey := []byte(r.URL.Query().Get(queryParamStartKey))
	endKey := []byte(r.URL.Query().Get(queryParamEndKey))

	sw := kv.NewStreamWriter(nopWriteCloser{w})
	for _, sst := range s.sortedSSTs {
		if !sst.Range.Overlaps(kv.KeyRange{StartKey: startKey, EndKey: endKey}) {
			continue
		}
		reader, err := kv.NewSSTReader(sst.Path, sst.Range)
		if err != nil {
			writeError(w, err)
			return
		}
		err = kv.Copy(sw, reader)
		_ = reader.Close()
		if err != nil {
			writeError(w, err)
			return
		}
	}
	_ = sw.Close()
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has been compacted"))
		return
	}

	streamReader := kv.NewStreamReader(r.Body)
	defer streamReader.Close()

	alloc := arena.NewAllocator(maxPendingSSTFileSize)
	var (
		keys      [][]byte
		vals      [][]byte
		totalSize int
		ssts      []*kv.SSTMeta
	)

	flush := func() error {
		sort.Sort(keySorter{keys: keys, vals: vals})
		sstPath := s.generateSSTPath()
		sstWriter, err := kv.NewSSTWriter(sstPath)
		if err != nil {
			return err
		}
		pcw := kv.NewPropertyCollectingWriter(sstWriter, 1<<20)

		for i := 0; i < len(keys); i++ {
			if err := pcw.Write(keys[i], vals[i]); err != nil {
				_ = pcw.Close()
				return err
			}
		}
		if err := pcw.Close(); err != nil {
			return err
		}
		ssts = append(ssts, &kv.SSTMeta{
			Path:       sstPath,
			Properties: pcw.Properties(),
		})
		return nil
	}

	for {
		key, val, err := streamReader.Read()
		if err != nil {
			writeError(w, fmt.Errorf("read key-value pairs: %w", err))
			return
		}
		if key == nil && totalSize > 0 || totalSize > maxPendingSSTFileSize {
			if err := flush(); err != nil {
				writeError(w, fmt.Errorf("write sst file: %w", err))
				return
			}
			keys = keys[:0]
			vals = vals[:0]
			totalSize = 0
			alloc.Reset()
		}
		if key == nil {
			break
		}
		newKey := alloc.Alloc(len(key))
		copy(newKey, key)
		newVal := alloc.Alloc(len(val))
		copy(newVal, val)
		keys = append(keys, newKey)
		vals = append(vals, newVal)
		totalSize += len(key) + len(val)
	}
	s.pendingSSTs.Lock()
	s.pendingSSTs.ssts = append(s.pendingSSTs.ssts, ssts...)
	s.pendingSSTs.Unlock()
}

type keySorter struct {
	keys [][]byte
	vals [][]byte
}

func (ks keySorter) Len() int {
	return len(ks.keys)
}

func (ks keySorter) Less(i, j int) bool {
	return bytes.Compare(ks.keys[i], ks.keys[j]) < 0
}

func (ks keySorter) Swap(i, j int) {
	ks.keys[i], ks.keys[j] = ks.keys[j], ks.keys[i]
	ks.vals[i], ks.vals[j] = ks.vals[j], ks.vals[i]
}

func (s *Server) handleCompact(_ http.ResponseWriter, _ *http.Request) {
	s.compactOnce.Do(func() {
		s.compact()
		s.compacted.Store(true)
	})
}

func (s *Server) handleImport(_ http.ResponseWriter, r *http.Request) {
	startKey := []byte(r.URL.Query().Get(queryParamStartKey))
	endKey := []byte(r.URL.Query().Get(queryParamEndKey))
	_, _ = startKey, endKey
}

func (s *Server) compact() {
	s.pendingSSTs.Lock()
	defer s.pendingSSTs.Unlock()

	ssts := s.pendingSSTs.ssts
	sort.Slice(ssts, func(i, j int) bool {
		return ssts[i].Range.Less(ssts[j].Range)
	})

	resultCh := make(chan *kv.SSTMeta, len(ssts))
	eg, _ := errgroup.WithContext(context.Background())
	eg.SetLimit(16)
	lastIdx := 0
	for i := 1; i < len(ssts); i++ {
		if !ssts[i].Range.Overlaps(ssts[lastIdx].Range) {
			overlappedSSTs := ssts[lastIdx:i]
			eg.Go(func() error {
				result, err := s.compactSSTs(overlappedSSTs)
				if err != nil {
					return err
				}
				resultCh <- result
				return nil
			})
		}
	}
	eg.Go(func() error {
		result, err := s.compactSSTs(ssts[lastIdx:])
		if err != nil {
			return err
		}
		if result != nil {
			resultCh <- result
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("compact sst files: %v", err)
	}

	close(resultCh)
	var results []*kv.SSTMeta
	for result := range resultCh {
		results = append(results, result)
	}
	s.sortedSSTs = results

	for _, sst := range ssts {
		_ = os.Remove(sst.Path)
	}
}

func (s *Server) compactSSTs(ssts []*kv.SSTMeta) (*kv.SSTMeta, error) {
	var readers []kv.Reader
	closeAll := func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}
	defer closeAll()

	for _, sst := range ssts {
		r, err := kv.NewSSTReader(sst.Path, sst.Range)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	mergedReader, err := kv.MergeReaders(readers...)
	if err != nil {
		return nil, err
	}

	sstPath := s.generateSSTPath()
	w, err := kv.NewSSTWriter(sstPath)
	if err != nil {
		return nil, err
	}
	pcw := kv.NewPropertyCollectingWriter(w, 1<<20)
	defer pcw.Close()

	if err := kv.Copy(pcw, mergedReader); err != nil {
		return nil, err
	}

	return &kv.SSTMeta{
		Path:       sstPath,
		Properties: pcw.Properties(),
	}, nil
}

func (s *Server) generateSSTPath() string {
	return fmt.Sprintf("%s/%d.sst", s.opts.dataDir, s.idGen.Add(1))
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
	log.Printf("server encountered error: %v", err)
}
