package opensearchexporter

import (
	"bytes"
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"io"

	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type opensearchTracesExporter struct {
	logger *zap.Logger

	index       string
	maxAttempts int

	client      *osClientCurrent
	bulkIndexer osBulkIndexerCurrent
	model       mappingModel
	bulkAction  string
}

func newTracesExporter(logger *zap.Logger, cfg *Config) (*opensearchTracesExporter, error) {
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

	return &opensearchTracesExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:       cfg.TracesIndex,
		bulkAction:  cfg.BulkAction,
		maxAttempts: maxAttempts,
		model:       model,
	}, nil
}

func (e *opensearchTracesExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *opensearchTracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
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

func (e *opensearchTracesExporter) pushTraceRecord(ctx context.Context, resource pcommon.Resource, record ptrace.Span) error {
	document, err := e.model.encodeSpan(resource, record)
	if err != nil {
		return fmt.Errorf("failed to encode trace record: %w", err)
	}
	return e.pushEvent(ctx, document)
}

func (e *opensearchTracesExporter) pushEvent(ctx context.Context, document []byte) error {
	attempts := 1
	body := bytes.NewReader(document)
	item := osBulkIndexerItem{Action: e.bulkAction, Index: e.index, Body: body}

	// Setup error handler. The handler handles the per item response status based on the
	// selective ACKing in the bulk response.
	item.OnFailure = func(ctx context.Context, item osBulkIndexerItem, resp osBulkIndexerResponseItem, err error) {
		switch {
		case attempts < e.maxAttempts && shouldRetryEvent(resp.Status):
			e.logger.Debug("Retrying to index event",
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status),
				zap.NamedError("reason", err))

			attempts++
			body.Seek(0, io.SeekStart)
			e.bulkIndexer.Add(ctx, item)

		case resp.Status == 0 && err != nil:
			// Encoding error. We didn't even attempt to send the event
			e.logger.Error("Drop event: failed to add event to the bulk request buffer.",
				zap.NamedError("reason", err))

		case err != nil:
			e.logger.Error("Drop event: failed to index event",
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status),
				zap.NamedError("reason", err))

		default:
			e.logger.Error(fmt.Sprintf("Drop event: failed to index event: %#v", resp.Error),
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status))
		}
	}

	return e.bulkIndexer.Add(ctx, item)
}
