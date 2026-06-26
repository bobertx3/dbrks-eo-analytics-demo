# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Logs from JSONL

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.bronze_logs AS
# MAGIC WITH raw AS (
# MAGIC   SELECT explode(resourceLogs) as rl
# MAGIC   FROM json.`/Volumes/bldemos/eo_analytics/raw_landing/logs/`
# MAGIC ),
# MAGIC with_scope AS (
# MAGIC   SELECT rl.resource.attributes as resource_attrs, explode(rl.scopeLogs) as sl FROM raw
# MAGIC ),
# MAGIC with_record AS (
# MAGIC   SELECT resource_attrs, sl.scope.name as scope_name, explode(sl.logRecords) as lr FROM with_scope
# MAGIC )
# MAGIC SELECT
# MAGIC   get(filter(resource_attrs, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
# MAGIC   get(filter(resource_attrs, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
# MAGIC   get(filter(resource_attrs, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
# MAGIC   get(filter(resource_attrs, x -> x.key = 'host.name'), 0).value.stringValue as host_name,
# MAGIC   scope_name,
# MAGIC   lr.severityNumber as severity_number,
# MAGIC   lr.severityText as severity_text,
# MAGIC   lr.body.stringValue as log_message,
# MAGIC   lr.traceId as trace_id,
# MAGIC   lr.spanId as span_id,
# MAGIC   get(filter(lr.attributes, x -> x.key = 'failure_pattern'), 0).value.stringValue as failure_pattern_id,
# MAGIC   CAST(lr.timeUnixNano AS BIGINT) as timestamp_unix_nano,
# MAGIC   to_timestamp(CAST(lr.timeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
# MAGIC   current_timestamp() as ingested_at
# MAGIC FROM with_record;
