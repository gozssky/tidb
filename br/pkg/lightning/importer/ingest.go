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
	"github.com/tikv/client-go/v2/tikv"
)

type Ingestor struct {
	kvStore *tikv.KVStore
}

func NewIngestor(kvStore *tikv.KVStore) *Ingestor {
	kvStore.Begin()
	return &Ingestor{
		kvStore: kvStore,
	}
}
