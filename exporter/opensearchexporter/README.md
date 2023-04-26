# Opensearch Exporter

This exporter supports sending OpenTelemetry logs to [OpenSearch](https://opensearch.org/).

## Configuration options

- `endpoints`: List of OpenSearch URLs. If endpoints is missing, the
  OPENSEARCH_URL environment variable will be used.
- `num_workers` (optional): Number of workers publishing bulk requests concurrently.
- `index`: The
  [index](https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/index/)
  or [datastream](https://opensearch.org/docs/latest/opensearch/data-streams/)
  name to publish events to. The default value is `logs-generic-default`.
- `bulk_action` (optional): the [action](https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/#request-body) for ingesting data. Only `create` and `index` are allowed here. 
- `pipeline` (optional): Optional [Ingest Node](https://opensearch.org/docs/latest/opensearch/rest-api/ingest-apis/get-ingest/)
  pipeline ID used for processing documents published by the exporter.
- `flush`: Event bulk buffer flush settings
  - `bytes` (default=5242880): Write buffer flush limit.
  - `interval` (default=30s): Write buffer time limit.
- `retry`: Event retry settings
  - `enabled` (default=true): Enable/Disable event retry on error. Retry
    support is enabled by default.
  - `max_requests` (default=3): Number of HTTP request retries.
  - `initial_interval` (default=100ms): Initial waiting time if a HTTP request failed.
  - `max_interval` (default=1m): Max waiting time if a HTTP request failed.
- `mapping`: Events are encoded to JSON. The `mapping` allows users to
  configure additional mapping rules.
  - `mode` (default=ecs): The fields naming mode. valid modes are:
    - `none`: Use original fields and event structure from the OTLP event.
    - `ecs`: Try to map fields defined in the
             [OpenTelemetry Semantic Conventions](https://github.com/open-telemetry/opentelemetry-specification/tree/main/semantic_conventions)
             to [Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html).
  - `fields` (optional): Configure additional fields mappings.
  - `file` (optional): Read additional field mappings from the provided YAML file.
  - `dedup` (default=true): Try to find and remove duplicate fields/attributes
    from events before publishing to OpenSearch. Some structured logging
    libraries can produce duplicate fields (for example zap). OpenSearch
    will reject documents that have duplicate fields.
  - `dedot` (default=true): When enabled attributes with `.` will be split into
    proper json objects.

### HTTP settings

- `read_buffer_size` (default=0): Read buffer size.
- `write_buffer_size` (default=0): Write buffer size used when.
- `timeout` (default=90s): HTTP request time limit.
- `headers` (optional): Headers to be send with each HTTP request.

### Security and Authentication settings

- `user` (optional): Username used for HTTP Basic Authentication.
- `password` (optional): Password used for HTTP Basic Authentication.

### TLS settings
- `ca_file` (optional): Root Certificate Authority (CA) certificate, for
  verifying the server's identity, if TLS is enabled.
- `cert_file` (optional): Client TLS certificate.
- `key_file` (optional): Client TLS key.
- `insecure` (optional): Disable verification of the server's identity, if TLS
  is enabled.

### Node Discovery

The OpenSearch Exporter will check OpenSearch regularly for available
nodes and updates the list of hosts if discovery is enabled. Newly discovered
nodes will automatically be used for load balancing.

- `discover`:
  - `on_start` (optional): If enabled the exporter queries OpenSearch
    for all known nodes in the cluster on startup.
  - `interval` (optional): Interval to update the list of OpenSearch nodes.

## Example

```yaml
exporters:
  opensearch:
    endpoints:
    - "https://localhost:9200"
```
