package opensearchexporter

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/sanitize"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type osClientCurrent = opensearch.Client
type osConfigCurrent = opensearch.Config
type osBulkIndexerCurrent = opensearchutil.BulkIndexer
type osBulkIndexerItem = opensearchutil.BulkIndexerItem
type osBulkIndexerResponseItem = opensearchutil.BulkIndexerResponseItem

// clientLogger implements the estransport.Logger interface
// that is required by the OpenSearch client for logging.
type clientLogger zap.Logger

var retryOnStatus = []int{500, 502, 503, 504, 429}

// LogRoundTrip should not modify the request or response, except for consuming and closing the body.
// Implementations have to check for nil values in request and response.
func (cl *clientLogger) LogRoundTrip(request *http.Request, response *http.Response, err error, _ time.Time, dur time.Duration) error {
	zl := (*zap.Logger)(cl)
	switch {
	case err == nil && response != nil:
		zl.Debug("Request roundtrip completed.",
			zap.String("path", sanitize.String(request.URL.Path)),
			zap.String("method", request.Method),
			zap.Duration("duration", dur),
			zap.String("status", response.Status))

	case err != nil:
		zl.Error("Request failed.", zap.NamedError("reason", err))
	}

	return nil
}

func newOpensearchClient(logger *zap.Logger, config *Config) (*osClientCurrent, error) {
	tlsCfg, err := config.TLSClientSetting.LoadTLSConfig()
	if err != nil {
		return nil, err
	}

	transport := newTransport(config, tlsCfg)

	var headers http.Header
	for k, v := range config.Headers {
		headers.Add(k, v)
	}

	// TODO: validate settings:
	//  - try to parse address and validate scheme (address must be a valid URL)

	// maxRetries configures the maximum number of event publishing attempts,
	// including the first send and additional retries.
	maxRetries := config.Retry.MaxRequests - 1
	retryDisabled := !config.Retry.Enabled || maxRetries <= 0
	if retryDisabled {
		maxRetries = 0
	}

	return opensearch.NewClient(osConfigCurrent{
		Transport: transport,

		// configure connection setup
		Addresses: config.Endpoints,
		Username:  config.Authentication.User,
		Password:  config.Authentication.Password,
		Header:    headers,

		// configure retry behavior
		RetryOnStatus:        retryOnStatus,
		DisableRetry:         retryDisabled,
		EnableRetryOnTimeout: config.Retry.Enabled,
		MaxRetries:           maxRetries,
		RetryBackoff:         createOpensearchBackoffFunc(&config.Retry),

		// configure sniffing
		DiscoverNodesOnStart:  config.Discovery.OnStart,
		DiscoverNodesInterval: config.Discovery.Interval,

		// configure internal metrics reporting and logging
		EnableMetrics:     false, // TODO
		EnableDebugLogger: false, // TODO
		Logger:            (*clientLogger)(logger),
	})
}

func newTransport(config *Config, tlsCfg *tls.Config) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		transport.TLSClientConfig = tlsCfg
	}
	if config.ReadBufferSize > 0 {
		transport.ReadBufferSize = config.ReadBufferSize
	}
	if config.WriteBufferSize > 0 {
		transport.WriteBufferSize = config.WriteBufferSize
	}

	return transport
}

func newBulkIndexer(logger *zap.Logger, client *opensearch.Client, config *Config) (osBulkIndexerCurrent, error) {
	return opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		NumWorkers:    config.NumWorkers,
		FlushBytes:    config.Flush.Bytes,
		FlushInterval: config.Flush.Interval,
		Client:        client,
		Pipeline:      config.Pipeline,
		Timeout:       config.Timeout,

		OnError: func(_ context.Context, err error) {
			logger.Error(fmt.Sprintf("Bulk indexer error: %v", err))
		},
	})
}

func createOpensearchBackoffFunc(config *RetrySettings) func(int) time.Duration {
	if !config.Enabled {
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	if config.InitialInterval > 0 {
		expBackoff.InitialInterval = config.InitialInterval
	}
	if config.MaxInterval > 0 {
		expBackoff.MaxInterval = config.MaxInterval
	}
	expBackoff.Reset()

	return func(attempts int) time.Duration {
		if attempts == 1 {
			expBackoff.Reset()
		}

		return expBackoff.NextBackOff()
	}
}

func shouldRetryEvent(status int) bool {
	for _, retryable := range retryOnStatus {
		if status == retryable {
			return true
		}
	}
	return false
}

// RequestBodyEnabled makes the client pass a copy of request body to the logger.
func (*clientLogger) RequestBodyEnabled() bool {
	// TODO: introduce setting log the bodies for more detailed debug logs
	return false
}

// ResponseBodyEnabled makes the client pass a copy of response body to the logger.
func (*clientLogger) ResponseBodyEnabled() bool {
	// TODO: introduce setting log the bodies for more detailed debug logs
	return false
}

func pushEvent(ctx context.Context, logger *zap.Logger, bulkAction string, index string, document []byte, bulkIndexer osBulkIndexerCurrent, maxAttempts int) error {
	attempts := 1
	body := bytes.NewReader(document)
	item := osBulkIndexerItem{Action: bulkAction, Index: index, Body: body}

	// Setup error handler. The handler handles the per item response status based on the
	// selective ACKing in the bulk response.
	item.OnFailure = func(ctx context.Context, item osBulkIndexerItem, resp osBulkIndexerResponseItem, err error) {
		switch {
		case attempts < maxAttempts && shouldRetryEvent(resp.Status):
			logger.Debug("Retrying to index event",
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status),
				zap.NamedError("reason", err))

			attempts++
			body.Seek(0, io.SeekStart)
			bulkIndexer.Add(ctx, item)

		case resp.Status == 0 && err != nil:
			// Encoding error. We didn't even attempt to send the event
			logger.Error("Drop event: failed to add event to the bulk request buffer.",
				zap.NamedError("reason", err))

		case err != nil:
			logger.Error("Drop event: failed to index event",
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status),
				zap.NamedError("reason", err))

		default:
			logger.Error(fmt.Sprintf("Drop event: failed to index event: %#v", resp.Error),
				zap.Int("attempt", attempts),
				zap.Int("status", resp.Status))
		}
	}

	return bulkIndexer.Add(ctx, item)
}
