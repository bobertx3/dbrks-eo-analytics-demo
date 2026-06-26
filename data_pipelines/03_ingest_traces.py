# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Traces from JSON

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.bronze_traces AS
# MAGIC WITH raw AS (
# MAGIC   SELECT explode(resourceSpans) as rs FROM json.`/Volumes/bldemos/eo_analytics/raw_landing/traces/`
# MAGIC ),
# MAGIC with_scope AS (SELECT explode(rs.scopeSpans) as ss FROM raw),
# MAGIC with_span AS (SELECT explode(ss.spans) as span FROM with_scope)
# MAGIC SELECT
# MAGIC   span.traceId as trace_id, span.spanId as span_id, span.parentSpanId as parent_span_id,
# MAGIC   span.name as operation_name, span.kind as span_kind,
# MAGIC   CAST(span.startTimeUnixNano AS BIGINT) as start_time_unix_nano,
# MAGIC   CAST(span.endTimeUnixNano AS BIGINT) as end_time_unix_nano,
# MAGIC   (CAST(span.endTimeUnixNano AS BIGINT) - CAST(span.startTimeUnixNano AS BIGINT)) / 1000000.0 as duration_ms,
# MAGIC   span.status.code as status_code, span.status.message as status_message,
# MAGIC   get(filter(span.attributes, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
# MAGIC   get(filter(span.attributes, x -> x.key = 'http.status_code'), 0).value.intValue as http_status_code,
# MAGIC   get(filter(span.attributes, x -> x.key = 'peer.service'), 0).value.stringValue as peer_service,
# MAGIC   get(filter(span.resource.attributes, x -> x.key = 'service.name'), 0).value.stringValue as resource_service_name,
# MAGIC   get(filter(span.resource.attributes, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
# MAGIC   get(filter(span.resource.attributes, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
# MAGIC   to_timestamp(CAST(span.startTimeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
# MAGIC   current_timestamp() as ingested_at
# MAGIC FROM with_span;
