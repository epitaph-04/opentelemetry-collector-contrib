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

// Package opensearchexporter contains an opentelemetry-collector exporter
// for OpenSearch.
package opensearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type opensearchLogsExporter struct {
	logger *zap.Logger

	index       string
	maxAttempts int

	client      *osClientCurrent
	bulkIndexer osBulkIndexerCurrent
	model       mappingModel
	bulkAction  string
}

func newLogsExporter(logger *zap.Logger, cfg *Config) (*opensearchLogsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, err := newOpensearchClient(logger, cfg)
	if err != nil {
		return nil, err
	}

	bulkIndexer, err := newBulkIndexer(logger, client, cfg)
	if err != nil {
		return nil, err
	}

	maxAttempts := 1
	if cfg.Retry.Enabled {
		maxAttempts = cfg.Retry.MaxRequests
	}

	// TODO: Apply encoding and field mapping settings.
	model := &encodeModel{dedup: true, dedot: false, flattenAttributes: false}
	if cfg.Mapping.Mode == MappingFlattenAttributes.String() {
		model.flattenAttributes = true
	}

	indexStr := cfg.LogsIndex
	if cfg.Index != "" {
		indexStr = cfg.Index
	}

	return &opensearchLogsExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:       indexStr,
		bulkAction:  cfg.BulkAction,
		maxAttempts: maxAttempts,
		model:       model,
	}, nil
}

func (e *opensearchLogsExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *opensearchLogsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	var errs []error

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		ills := rl.ScopeLogs()
		for j := 0; j < ills.Len(); j++ {
			logs := ills.At(j).LogRecords()
			for k := 0; k < logs.Len(); k++ {
				if err := e.pushLogRecord(ctx, resource, logs.At(k)); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}

					errs = append(errs, err)
				}
			}
		}
	}

	return multierr.Combine(errs...)
}

func (e *opensearchLogsExporter) pushLogRecord(ctx context.Context, resource pcommon.Resource, record plog.LogRecord) error {
	document, err := e.model.encodeLog(resource, record)
	if err != nil {
		return fmt.Errorf("failed to encode log event: %w", err)
	}
	return pushEvent(ctx, e.logger, e.bulkAction, e.index, document, e.bulkIndexer, e.maxAttempts)
}
