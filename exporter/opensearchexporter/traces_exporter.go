package opensearchexporter

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type opensearchTraceExporter struct {
	logger *zap.Logger

	index       string
	maxAttempts int

	client      *osClientCurrent
	bulkIndexer osBulkIndexerCurrent
	model       mappingModel
	bulkAction  string
}

func newTraceExporter(logger *zap.Logger, cfg *Config) (*opensearchTraceExporter, error) {
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

	return &opensearchTraceExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:       cfg.TracesIndex,
		bulkAction:  cfg.BulkAction,
		maxAttempts: maxAttempts,
		model:       model,
	}, nil
}

func (e *opensearchTraceExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *opensearchTraceExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	var errs []error

	rls := td.ResourceSpans()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		ills := rl.ScopeSpans()
		for j := 0; j < ills.Len(); j++ {
			spans := ills.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if err := e.pushTraceRecord(ctx, resource, spans.At(k)); err != nil {
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

func (e *opensearchTraceExporter) pushTraceRecord(ctx context.Context, resource pcommon.Resource, record ptrace.Span) error {
	document, err := e.model.encodeSpan(resource, record)
	if err != nil {
		return fmt.Errorf("failed to encode log event: %w", err)
	}
	return pushEvent(ctx, e.logger, e.bulkAction, e.index, document, e.bulkIndexer, e.maxAttempts)
}
