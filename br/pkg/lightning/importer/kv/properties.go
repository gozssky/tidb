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

type Properties struct {
	Range   KeyRange      `json:"range"`
	Count   int           `json:"count"`
	Length  int64         `json:"length"`
	Samples []SamplePoint `json:"samples"`
}

type SamplePoint struct {
	Key    []byte `json:"key"`
	Val    []byte `json:"val"`
	Count  int    `json:"count"`
	Length int64  `json:"length"`
}

type PropertyCollectingWriter struct {
	w                Writer
	sampleDistance   int64
	samples          []SamplePoint
	firstKey         []byte
	lastKey          []byte
	count            int
	length           int64
	lastSampleCount  int
	lastSampleLength int64
}

func NewPropertyCollectingWriter(w Writer, sampleDistance int64) *PropertyCollectingWriter {
	return &PropertyCollectingWriter{w: w, sampleDistance: sampleDistance}
}

func (pcw *PropertyCollectingWriter) Write(key, val []byte) error {
	if err := pcw.w.Write(key, val); err != nil {
		return err
	}

	if pcw.firstKey == nil {
		pcw.firstKey = append(pcw.firstKey[:0], key...)
	}
	pcw.lastKey = append(pcw.lastKey[:0], key...)
	pcw.count++
	pcw.length += int64(len(key) + len(val))
	if pcw.length-pcw.lastSampleLength >= pcw.sampleDistance {
		pcw.samples = append(pcw.samples, SamplePoint{
			Key:    append([]byte(nil), key...),
			Val:    append([]byte(nil), val...),
			Count:  pcw.count - pcw.lastSampleCount,
			Length: pcw.length - pcw.lastSampleLength,
		})
		pcw.lastSampleCount = pcw.count
		pcw.lastSampleLength = pcw.length
	}
	return nil
}

func (pcw *PropertyCollectingWriter) Close() error {
	return pcw.w.Close()
}

func (pcw *PropertyCollectingWriter) Properties() *Properties {
	return &Properties{
		Range: KeyRange{
			StartKey: pcw.firstKey,
			EndKey:   nextKey(pcw.lastKey),
		},
		Count:   pcw.count,
		Length:  pcw.length,
		Samples: pcw.samples,
	}
}
