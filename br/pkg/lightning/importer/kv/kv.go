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

package kv

import (
	"bytes"
	"container/heap"
)

// KeyRange represents a range of keys.
// StartKey is inclusive, while EndKey is exclusive.
// If EndKey is nil, it means the range is unbounded.
type KeyRange struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
}

// Overlaps returns true if the two ranges overlap.
func (kr KeyRange) Overlaps(other KeyRange) bool {
	if kr.EndKey != nil && bytes.Compare(kr.EndKey, other.StartKey) <= 0 {
		return false
	}
	if other.EndKey != nil && bytes.Compare(other.EndKey, kr.StartKey) <= 0 {
		return false
	}
	return true
}

func (kr KeyRange) Less(other KeyRange) bool {
	cmp := bytes.Compare(kr.StartKey, other.StartKey)
	if cmp != 0 {
		return cmp < 0
	}
	return kr.EndKey != nil && (other.EndKey == nil || bytes.Compare(kr.EndKey, other.EndKey) < 0)
}

// Reader is the interface for reading key-value pairs.
type Reader interface {
	// Read reads the next key-value pair. If there is no more pair, both key and value will be nil.
	Read() (key, val []byte, err error)
	// Close closes the reader.
	Close() error
}

// Writer is an interface for writing key-value pairs.
type Writer interface {
	Write(key, val []byte) error
	Close() error
}

// Copy copies all key-value pairs from reader to writer.
func Copy(w Writer, r Reader) error {
	for {
		key, val, err := r.Read()
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}
		if err := w.Write(key, val); err != nil {
			return err
		}
	}
}

type mergedReader struct {
	h mergedReaderHeap
}

// MergeReaders merges multiple ordered readers into one ordered reader.
// The input readers must be ordered by key. Otherwise, the result is undefined.
func MergeReaders(readers ...Reader) (Reader, error) {
	h := make(mergedReaderHeap, 0, len(readers))
	for _, reader := range readers {
		key, val, err := reader.Read()
		if err != nil {
			return nil, err
		}
		if key == nil {
			if err := reader.Close(); err != nil {
				return nil, err
			}
			continue
		}
		h = append(h, mergedReaderItem{reader: reader, firstKey: key, firstVal: val, firstValid: true})
	}
	heap.Init(&h)
	return &mergedReader{h: h}, nil
}

func (mr *mergedReader) Read() (key, val []byte, err error) {
	if mr.h.Len() == 0 {
		return nil, nil, nil
	}
	for {
		if mr.h[0].firstValid {
			mr.h[0].firstValid = false
			return mr.h[0].firstKey, mr.h[0].firstVal, nil
		}
		key, val, err := mr.h[0].reader.Read()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			if err := mr.h[0].reader.Close(); err != nil {
				return nil, nil, err
			}
			heap.Pop(&mr.h)
		} else {
			mr.h[0].firstKey = key
			mr.h[0].firstVal = val
			mr.h[0].firstValid = true
			heap.Fix(&mr.h, 0)
		}
	}
}

func (mr *mergedReader) Close() error {
	var firstErr error
	for _, item := range mr.h {
		if err := item.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type mergedReaderItem struct {
	firstKey   []byte
	firstVal   []byte
	firstValid bool
	reader     Reader
}

type mergedReaderHeap []mergedReaderItem

func (h mergedReaderHeap) Len() int {
	return len(h)
}

func (h mergedReaderHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].firstKey, h[j].firstKey) < 0
}

func (h mergedReaderHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergedReaderHeap) Push(x interface{}) {
	*h = append(*h, x.(mergedReaderItem))
}

func (h *mergedReaderHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
