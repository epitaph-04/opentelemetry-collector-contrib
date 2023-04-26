// Copyright 2020, OpenTelemetry Authors
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

package opensearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
)

// Config defines configuration for Opensearch exporter.
type Config struct {

	// Endpoints holds the OpenSearch URLs the exporter should send events to.
	//
	// This setting is required if CloudID is not set and if the
	// OPENSEARCH_URL environment variable is not set.
	Endpoints []string `mapstructure:"endpoints"`

	// NumWorkers configures the number of workers publishing bulk requests.
	NumWorkers int `mapstructure:"num_workers"`

	// Index configures the index, index alias, or data stream name events should be indexed in.
	//
	// https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/index/
	// https://opensearch.org/docs/latest/opensearch/data-streams/
	//
	// This setting is required.
	Index string `mapstructure:"index"`

	// BulkAction configures the action for ingesting data. Only `create` and `index` are allowed here.
	//
	// https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/#request-body
	//
	// If not specified, the default value `create` will be used.
	BulkAction string `mapstructure:"bulk_action"`

	// Pipeline configures the ingest node pipeline name that should be used to process the
	// events.
	//
	// https://opensearch.org/docs/latest/opensearch/rest-api/ingest-apis/get-ingest/
	Pipeline string `mapstructure:"pipeline"`

	HTTPClientSettings `mapstructure:",squash"`
	Discovery          DiscoverySettings `mapstructure:"discover"`
	Retry              RetrySettings     `mapstructure:"retry"`
	Flush              FlushSettings     `mapstructure:"flush"`
	Mapping            MappingsSettings  `mapstructure:"mapping"`
}

type HTTPClientSettings struct {
	Authentication AuthenticationSettings `mapstructure:",squash"`

	// ReadBufferSize for HTTP client. See http.Transport.ReadBufferSize.
	ReadBufferSize int `mapstructure:"read_buffer_size"`

	// WriteBufferSize for HTTP client. See http.Transport.WriteBufferSize.
	WriteBufferSize int `mapstructure:"write_buffer_size"`

	// Timeout configures the HTTP request timeout.
	Timeout time.Duration `mapstructure:"timeout"`

	// Headers allows users to configure optional HTTP headers that
	// will be send with each HTTP request.
	Headers map[string]string `mapstructure:"headers,omitempty"`

	configtls.TLSClientSetting `mapstructure:"tls,omitempty"`
}

// AuthenticationSettings defines user authentication related settings.
type AuthenticationSettings struct {
	// User is used to configure HTTP Basic Authentication.
	User string `mapstructure:"user"`

	// Password is used to configure HTTP Basic Authentication.
	Password string `mapstructure:"password"`
}

// DiscoverySettings defines OpenSearch node discovery related settings.
// The exporter will check OpenSearch regularly for available nodes
// and updates the list of hosts if discovery is enabled. Newly discovered
// nodes will automatically be used for load balancing.
//
// DiscoverySettings should not be enabled when operating OpenSearch behind a proxy
// or load balancer.
type DiscoverySettings struct {
	// OnStart, if set, instructs the exporter to look for available OpenSearch
	// nodes the first time the exporter connects to the cluster.
	OnStart bool `mapstructure:"on_start"`

	// Interval instructs the exporter to renew the list of OpenSearch URLs
	// with the given interval. URLs will not be updated if Interval is <=0.
	Interval time.Duration `mapstructure:"interval"`
}

// FlushSettings  defines settings for configuring the write buffer flushing
// policy in the OpenSearch exporter. The exporter sends a bulk request with
// all events already serialized into the send-buffer.
type FlushSettings struct {
	// Bytes sets the send buffer flushing limit.
	Bytes int `mapstructure:"bytes"`

	// Interval configures the max age of a document in the send buffer.
	Interval time.Duration `mapstructure:"interval"`
}

// RetrySettings defines settings for the HTTP request retries in the OpenSearch exporter.
// Failed sends are retried with exponential backoff.
type RetrySettings struct {
	// Enabled allows users to disable retry without having to comment out all settings.
	Enabled bool `mapstructure:"enabled"`

	// MaxRequests configures how often an HTTP request is retried before it is assumed to be failed.
	MaxRequests int `mapstructure:"max_requests"`

	// InitialInterval configures the initial waiting time if a request failed.
	InitialInterval time.Duration `mapstructure:"initial_interval"`

	// MaxInterval configures the max waiting time if consecutive requests failed.
	MaxInterval time.Duration `mapstructure:"max_interval"`
}

type MappingsSettings struct {
	// Mode configures the field mappings.
	Mode string `mapstructure:"mode"`

	// Additional field mappings.
	Fields map[string]string `mapstructure:"fields"`

	// File to read additional fields mappings from.
	File string `mapstructure:"file"`

	// Try to find and remove duplicate fields
	Dedup bool `mapstructure:"dedup"`

	Dedot bool `mapstructure:"dedot"`
}

type MappingMode int

// Enum values for MappingMode.
const (
	MappingNone MappingMode = iota
	MappingECS
	MappingFlattenAttributes
)

var (
	errConfigNoEndpoint        = errors.New("endpoints must be specified")
	errConfigEmptyEndpoint     = errors.New("endpoints must not include empty entries")
	errConfigNoIndex           = errors.New("index must be specified")
	errConfigInvalidBulkAction = errors.New("bulk_action can either be `create` or `index`")
)

func (m MappingMode) String() string {
	switch m {
	case MappingNone:
		return ""
	case MappingECS:
		return "ecs"
	case MappingFlattenAttributes:
		return "flatten_attributes"
	default:
		return ""
	}
}

var mappingModes = func() map[string]MappingMode {
	table := map[string]MappingMode{}
	for _, m := range []MappingMode{
		MappingNone,
		MappingECS,
		MappingFlattenAttributes,
	} {
		table[strings.ToLower(m.String())] = m
	}

	// config aliases
	table["no"] = MappingNone
	table["none"] = MappingNone

	return table
}()

const defaultOpenSearchEnvName = "OPENSEARCH_URL"

// Validate validates the OpenSearch server configuration.
func (cfg *Config) Validate() error {
	if len(cfg.Endpoints) == 0 {
		if os.Getenv(defaultOpenSearchEnvName) == "" {
			return errConfigNoEndpoint
		}
	}

	if len(cfg.BulkAction) == 0 {
		cfg.BulkAction = "create"
	}

	if cfg.BulkAction != "create" && cfg.BulkAction != "index" {
		return errConfigInvalidBulkAction
	}

	for _, endpoint := range cfg.Endpoints {
		if endpoint == "" {
			return errConfigEmptyEndpoint
		}
	}

	if cfg.Index == "" {
		return errConfigNoIndex
	}

	if _, ok := mappingModes[cfg.Mapping.Mode]; !ok {
		return fmt.Errorf("unknown mapping mode %v", cfg.Mapping.Mode)
	}

	return nil
}
