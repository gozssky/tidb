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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/pingcap/tidb/br/pkg/lightning/importer/kv"
)

type Client struct {
	endpoint string
}

func NewClient(endpoint string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	return &Client{endpoint: u.String()}, nil
}

func (c *Client) NewReader(ctx context.Context, startKey, endKey []byte) (kv.Reader, error) {
	baseURL, _ := url.Parse(c.endpoint)
	baseURL.Path = "/read"
	params := url.Values{}
	params.Set(queryParamStartKey, string(startKey))
	params.Set(queryParamEndKey, string(endKey))
	baseURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := c.sendRequestWithResponse(req)
	if err != nil {
		return nil, err
	}
	return kv.NewStreamReader(resp.Body), nil
}

func (c *Client) NewWriter(ctx context.Context) (kv.Writer, error) {
	baseURL, _ := url.Parse(c.endpoint)
	baseURL.Path = "/write"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL.String(), nil)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error, 1)
	pr, pw := io.Pipe()
	go func() {
		defer pr.Close()
		req.Body = pr
		errCh <- c.sendRequest(req)
	}()

	return kv.NewStreamWriter(pw), nil
}

func (c *Client) Compact(ctx context.Context) error {
	baseURL, _ := url.Parse(c.endpoint)
	baseURL.Path = "/write"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL.String(), nil)
	if err != nil {
		return err
	}
	return c.sendRequest(req)
}

func (c *Client) Ingest(ctx context.Context, startKey, endKey []byte) error {
	baseURL, _ := url.Parse(c.endpoint)
	baseURL.Path = "/write"
	params := url.Values{}
	params.Set(queryParamStartKey, string(startKey))
	params.Set(queryParamStartKey, string(startKey))
	params.Set(queryParamEndKey, string(endKey))
	baseURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL.String(), nil)
	if err != nil {
		return err
	}
	return c.sendRequest(req)
}

func (c *Client) sendRequest(req *http.Request) error {
	resp, err := c.sendRequestWithResponse(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (c *Client) sendRequestWithResponse(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("status code %d: %s", resp.StatusCode, string(buf))
	}
	return resp, nil
}
