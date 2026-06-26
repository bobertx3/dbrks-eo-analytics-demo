"""
01_create_bronze_tables.py
Creates Bronze (raw/parsed) Delta tables.

Metrics are now protobuf binary (.pb) files -- we decode them using the same
wire format encoder/decoder from the generator, flatten to rows, and batch-insert
via SQL.

Logs, traces, and events are still JSONL/JSON and use the json.`path` SQL approach.
Network flows use SQL-generated synthetic data (matching the .pb schema).
"""
import os
import io
import struct
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"


def get_warehouse_id(w):
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING",) and wh.enable_serverless_compute:
            print(f"  Using serverless warehouse: {wh.name} ({wh.id})")
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING",):
            print(f"  Using warehouse: {wh.name} ({wh.id})")
            return wh.id
    if warehouses:
        wh = warehouses[0]
        print(f"  Starting warehouse: {wh.name} ({wh.id})")
        w.warehouses.start(wh.id)
        time.sleep(30)
        return wh.id
    raise RuntimeError("No SQL warehouse found.")


def execute_sql(w, warehouse_id, sql, description=""):
    """Execute SQL statement and wait for completion."""
    if description:
        print(f"  {description} ...")
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        catalog=CATALOG,
        schema=SCHEMA,
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        return resp
    elif resp.status and resp.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    else:
        stmt_id = resp.statement_id
        for _ in range(120):
            time.sleep(10)
            status = w.statement_execution.get_statement(stmt_id)
            if status.status.state == StatementState.SUCCEEDED:
                return status
            if status.status.state == StatementState.FAILED:
                raise RuntimeError(f"SQL failed: {status.status.error}")
        raise RuntimeError("SQL timed out")


# ============================================================================
# PROTOBUF WIRE FORMAT DECODER for OTLP Metrics
# Decodes binary data matching otlp_metrics.proto without needing protoc
# ============================================================================

class ProtobufDecoder:
    """Minimal protobuf wire format decoder."""

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def _read_varint(self):
        result = 0
        shift = 0
        while True:
            if self.pos >= len(self.data):
                raise EOFError("Unexpected end of data reading varint")
            b = self.data[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        return result

    def _read_fixed64(self):
        val = struct.unpack_from("<Q", self.data, self.pos)[0]
        self.pos += 8
        return val

    def _read_sfixed64(self):
        val = struct.unpack_from("<q", self.data, self.pos)[0]
        self.pos += 8
        return val

    def _read_double(self):
        val = struct.unpack_from("<d", self.data, self.pos)[0]
        self.pos += 8
        return val

    def _read_fixed32(self):
        val = struct.unpack_from("<I", self.data, self.pos)[0]
        self.pos += 4
        return val

    def _read_bytes(self, length):
        val = self.data[self.pos:self.pos + length]
        self.pos += length
        return val

    def read_field(self):
        """Read one field tag + value. Returns (field_number, wire_type, value)."""
        if self.pos >= len(self.data):
            return None
        tag = self._read_varint()
        field_number = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # varint
            value = self._read_varint()
        elif wire_type == 1:  # 64-bit (fixed64/sfixed64/double)
            value = self.data[self.pos:self.pos + 8]
            self.pos += 8
        elif wire_type == 2:  # length-delimited
            length = self._read_varint()
            value = self._read_bytes(length)
        elif wire_type == 5:  # 32-bit
            value = self._read_fixed32()
        else:
            raise ValueError(f"Unknown wire type {wire_type}")

        return (field_number, wire_type, value)

    def read_all_fields(self):
        """Read all fields into a list of (field_number, wire_type, value)."""
        fields = []
        while self.pos < len(self.data):
            f = self.read_field()
            if f is None:
                break
            fields.append(f)
        return fields


def decode_string(data):
    return data.decode("utf-8")


def decode_key_value(data):
    """Decode a KeyValue message -> (key, value_dict)."""
    dec = ProtobufDecoder(data)
    key = ""
    value = {}
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:  # key
            key = decode_string(val)
        elif fn == 2 and wt == 2:  # value (AnyValue)
            value = decode_any_value(val)
    return key, value


def decode_any_value(data):
    """Decode AnyValue -> dict with one of string_value/bool_value/int_value/double_value."""
    dec = ProtobufDecoder(data)
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            return {"string_value": decode_string(val)}
        elif fn == 2 and wt == 0:
            return {"bool_value": bool(val)}
        elif fn == 3 and wt == 0:
            return {"int_value": val}
        elif fn == 4 and wt == 1:
            return {"double_value": struct.unpack("<d", val)[0]}
    return {}


def decode_number_data_point(data):
    """Decode NumberDataPoint -> dict."""
    dec = ProtobufDecoder(data)
    dp = {"attributes": [], "time_unix_nano": 0}
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            k, v = decode_key_value(val)
            dp["attributes"].append({"key": k, "value": v})
        elif fn == 2 and wt == 1:
            dp["time_unix_nano"] = struct.unpack("<Q", val)[0]
        elif fn == 4 and wt == 1:
            dp["as_double"] = struct.unpack("<d", val)[0]
        elif fn == 6 and wt == 1:
            dp["as_int"] = struct.unpack("<q", val)[0]
    return dp


def decode_histogram_data_point(data):
    """Decode HistogramDataPoint -> dict."""
    dec = ProtobufDecoder(data)
    dp = {"attributes": [], "time_unix_nano": 0, "count": 0, "sum": 0.0,
          "bucket_counts": [], "explicit_bounds": []}
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            k, v = decode_key_value(val)
            dp["attributes"].append({"key": k, "value": v})
        elif fn == 2 and wt == 1:
            dp["time_unix_nano"] = struct.unpack("<Q", val)[0]
        elif fn == 4 and wt == 0:
            dp["count"] = val
        elif fn == 5 and wt == 1:
            dp["sum"] = struct.unpack("<d", val)[0]
        elif fn == 6 and wt == 2:
            # packed repeated uint64
            sub = ProtobufDecoder(val)
            while sub.pos < len(val):
                dp["bucket_counts"].append(sub._read_varint())
        elif fn == 7 and wt == 2:
            # packed repeated double
            i = 0
            while i + 8 <= len(val):
                dp["explicit_bounds"].append(struct.unpack_from("<d", val, i)[0])
                i += 8
    return dp


def decode_metric(data):
    """Decode Metric -> dict with name, unit, type, and data points."""
    dec = ProtobufDecoder(data)
    metric = {"name": "", "unit": "", "type": "unknown", "data_points": []}
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            metric["name"] = decode_string(val)
        elif fn == 3 and wt == 2:
            metric["unit"] = decode_string(val)
        elif fn == 5 and wt == 2:  # gauge
            metric["type"] = "gauge"
            gauge_dec = ProtobufDecoder(val)
            for gfn, gwt, gval in gauge_dec.read_all_fields():
                if gfn == 1 and gwt == 2:
                    metric["data_points"].append(decode_number_data_point(gval))
        elif fn == 7 and wt == 2:  # sum
            metric["type"] = "sum"
            sum_dec = ProtobufDecoder(val)
            for sfn, swt, sval in sum_dec.read_all_fields():
                if sfn == 1 and swt == 2:
                    metric["data_points"].append(decode_number_data_point(sval))
        elif fn == 9 and wt == 2:  # histogram
            metric["type"] = "histogram"
            hist_dec = ProtobufDecoder(val)
            for hfn, hwt, hval in hist_dec.read_all_fields():
                if hfn == 1 and hwt == 2:
                    metric["data_points"].append(decode_histogram_data_point(hval))
    return metric


def decode_scope_metrics(data):
    """Decode ScopeMetrics -> (scope_name, [metrics])."""
    dec = ProtobufDecoder(data)
    scope_name = ""
    metrics = []
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            # InstrumentationScope
            scope_dec = ProtobufDecoder(val)
            for sfn, swt, sval in scope_dec.read_all_fields():
                if sfn == 1 and swt == 2:
                    scope_name = decode_string(sval)
        elif fn == 2 and wt == 2:
            metrics.append(decode_metric(val))
    return scope_name, metrics


def decode_resource(data):
    """Decode Resource -> dict of attributes."""
    dec = ProtobufDecoder(data)
    attrs = {}
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            k, v = decode_key_value(val)
            if "string_value" in v:
                attrs[k] = v["string_value"]
            elif "int_value" in v:
                attrs[k] = str(v["int_value"])
            elif "double_value" in v:
                attrs[k] = str(v["double_value"])
            elif "bool_value" in v:
                attrs[k] = str(v["bool_value"])
    return attrs


def decode_resource_metrics(data):
    """Decode ResourceMetrics -> (resource_attrs, [(scope_name, [metrics])])."""
    dec = ProtobufDecoder(data)
    resource_attrs = {}
    scope_metrics_list = []
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            resource_attrs = decode_resource(val)
        elif fn == 2 and wt == 2:
            scope_metrics_list.append(decode_scope_metrics(val))
    return resource_attrs, scope_metrics_list


def decode_metrics_data(data):
    """Decode MetricsData (top-level) -> list of (resource_attrs, scope_metrics)."""
    dec = ProtobufDecoder(data)
    resource_metrics_list = []
    for fn, wt, val in dec.read_all_fields():
        if fn == 1 and wt == 2:
            resource_metrics_list.append(decode_resource_metrics(val))
    return resource_metrics_list


def flatten_metrics_pb(pb_data):
    """Decode a .pb file and flatten into rows for bronze_metrics table.
    Returns list of tuples matching the bronze_metrics schema."""
    rows = []
    resource_metrics_list = decode_metrics_data(pb_data)

    for resource_attrs, scope_metrics_entries in resource_metrics_list:
        service_name = resource_attrs.get("service.name", "")
        environment = resource_attrs.get("deployment.environment", "")
        region = resource_attrs.get("cloud.region", "")
        host_name = resource_attrs.get("host.name", "")
        service_version = resource_attrs.get("service.version", "")

        for scope_name, metrics in scope_metrics_entries:
            for metric in metrics:
                metric_name = metric["name"]
                metric_unit = metric["unit"]
                metric_type = metric["type"]

                for dp in metric["data_points"]:
                    time_nano = dp.get("time_unix_nano", 0)

                    if metric_type == "histogram":
                        metric_value = None
                        histogram_count = dp.get("count", 0)
                        histogram_sum = dp.get("sum", 0.0)
                    else:
                        metric_value = dp.get("as_double")
                        if metric_value is None and "as_int" in dp:
                            metric_value = float(dp["as_int"])
                        histogram_count = None
                        histogram_sum = None

                    rows.append((
                        service_name, environment, region, host_name, service_version,
                        scope_name, metric_name, metric_unit,
                        metric_value, histogram_count, histogram_sum,
                        metric_type, time_nano
                    ))
    return rows


def ingest_protobuf_metrics(w, warehouse_id):
    """Read .pb metric files from volume, decode, and insert into bronze_metrics."""
    print("  Ingesting protobuf metrics from volume ...")

    # Create the table schema first
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_metrics (
      service_name STRING,
      environment STRING,
      region STRING,
      host_name STRING,
      service_version STRING,
      scope_name STRING,
      metric_name STRING,
      metric_unit STRING,
      metric_value DOUBLE,
      histogram_count BIGINT,
      histogram_sum DOUBLE,
      metric_type STRING,
      timestamp_unix_nano BIGINT,
      event_timestamp TIMESTAMP,
      ingested_at TIMESTAMP
    )
    """, "Creating bronze_metrics schema")

    # List .pb files in the metrics directory
    pb_files = []
    try:
        for entry in w.files.list_directory_contents(f"{VOLUME_PATH}/metrics/"):
            if entry.path and entry.path.endswith(".pb"):
                pb_files.append(entry.path)
    except Exception as e:
        print(f"  Warning: Could not list metrics directory: {e}")
        print("  Falling back to known file pattern ...")
        # Fallback: try known file names
        for i in range(15):
            pb_files.append(f"{VOLUME_PATH}/metrics/metrics_{i:02d}_*.pb")

    if not pb_files:
        print("  No .pb files found in metrics directory.")
        return

    total_rows = 0
    for pb_path in pb_files:
        try:
            # Download the .pb file
            resp = w.files.download(pb_path)
            pb_data = resp.contents.read()
            resp.contents.close()

            # Decode and flatten
            rows = flatten_metrics_pb(pb_data)
            if not rows:
                continue

            # Insert in batches of 5000 via VALUES clause
            batch_size = 5000
            for batch_start in range(0, len(rows), batch_size):
                batch = rows[batch_start:batch_start + batch_size]
                values_parts = []
                for row in batch:
                    svc, env, reg, host, ver, scope, mname, munit, mval, hcount, hsum, mtype, tnano = row
                    mval_str = str(mval) if mval is not None else "NULL"
                    hcount_str = str(hcount) if hcount is not None else "NULL"
                    hsum_str = str(hsum) if hsum is not None else "NULL"
                    # Escape single quotes in strings
                    svc = svc.replace("'", "''")
                    host = host.replace("'", "''")
                    values_parts.append(
                        f"('{svc}', '{env}', '{reg}', '{host}', '{ver}', "
                        f"'{scope}', '{mname}', '{munit}', "
                        f"{mval_str}, {hcount_str}, {hsum_str}, "
                        f"'{mtype}', {tnano}, "
                        f"to_timestamp({tnano} / 1000000000), current_timestamp())"
                    )

                values_sql = ",\n".join(values_parts)
                execute_sql(w, warehouse_id, f"""
                INSERT INTO {CATALOG}.{SCHEMA}.bronze_metrics VALUES
                {values_sql}
                """)

            total_rows += len(rows)
            fname = pb_path.split("/")[-1]
            print(f"    Ingested {fname}: {len(rows):,} rows")

        except Exception as e:
            fname = pb_path.split("/")[-1] if "/" in pb_path else pb_path
            print(f"    Error ingesting {fname}: {e}")

    print(f"  Metrics ingestion complete: {total_rows:,} total rows.")


def main():
    w = WorkspaceClient(profile=PROFILE)
    warehouse_id = get_warehouse_id(w)

    print(f"\nCreating Bronze tables in {CATALOG}.{SCHEMA} ...")

    # ── bronze_metrics (from protobuf) ─────────────────────────────
    ingest_protobuf_metrics(w, warehouse_id)

    # ── bronze_logs ────────────────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_logs AS
    WITH raw AS (
      SELECT explode(resourceLogs) as rl
      FROM json.`{VOLUME_PATH}/logs/`
    ),
    with_scope AS (
      SELECT
        rl.resource.attributes as resource_attrs,
        explode(rl.scopeLogs) as sl
      FROM raw
    ),
    with_record AS (
      SELECT
        resource_attrs,
        sl.scope.name as scope_name,
        explode(sl.logRecords) as lr
      FROM with_scope
    )
    SELECT
      get(filter(resource_attrs, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
      get(filter(resource_attrs, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
      get(filter(resource_attrs, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
      get(filter(resource_attrs, x -> x.key = 'host.name'), 0).value.stringValue as host_name,
      scope_name,
      lr.severityNumber as severity_number,
      lr.severityText as severity_text,
      lr.body.stringValue as log_message,
      lr.traceId as trace_id,
      lr.spanId as span_id,
      get(filter(lr.attributes, x -> x.key = 'failure_pattern'), 0).value.stringValue as failure_pattern_id,
      CAST(lr.timeUnixNano AS BIGINT) as timestamp_unix_nano,
      to_timestamp(CAST(lr.timeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
      current_timestamp() as ingested_at
    FROM with_record
    """, "Creating bronze_logs")

    # ── bronze_traces ──────────────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_traces AS
    WITH raw AS (
      SELECT explode(resourceSpans) as rs
      FROM json.`{VOLUME_PATH}/traces/`
    ),
    with_scope AS (
      SELECT explode(rs.scopeSpans) as ss
      FROM raw
    ),
    with_span AS (
      SELECT explode(ss.spans) as span
      FROM with_scope
    )
    SELECT
      span.traceId as trace_id,
      span.spanId as span_id,
      span.parentSpanId as parent_span_id,
      span.name as operation_name,
      span.kind as span_kind,
      CAST(span.startTimeUnixNano AS BIGINT) as start_time_unix_nano,
      CAST(span.endTimeUnixNano AS BIGINT) as end_time_unix_nano,
      (CAST(span.endTimeUnixNano AS BIGINT) - CAST(span.startTimeUnixNano AS BIGINT)) / 1000000.0 as duration_ms,
      span.status.code as status_code,
      span.status.message as status_message,
      get(filter(span.attributes, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
      get(filter(span.attributes, x -> x.key = 'http.status_code'), 0).value.intValue as http_status_code,
      get(filter(span.attributes, x -> x.key = 'peer.service'), 0).value.stringValue as peer_service,
      get(filter(span.resource.attributes, x -> x.key = 'service.name'), 0).value.stringValue as resource_service_name,
      get(filter(span.resource.attributes, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
      get(filter(span.resource.attributes, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
      to_timestamp(CAST(span.startTimeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
      current_timestamp() as ingested_at
    FROM with_span
    """, "Creating bronze_traces")

    # ── bronze_incidents ───────────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_incidents AS
    SELECT
      incident_id,
      title,
      description,
      severity,
      status,
      CAST(created_at AS TIMESTAMP) as created_at,
      CAST(resolved_at AS TIMESTAMP) as resolved_at,
      CAST(mttr_minutes AS INT) as mttr_minutes,
      root_service,
      impacted_services,
      CAST(blast_radius AS INT) as blast_radius,
      domain,
      failure_pattern_id,
      failure_pattern_name,
      environment,
      region,
      CAST(revenue_impact_usd AS DOUBLE) as revenue_impact_usd,
      CAST(sla_breached AS BOOLEAN) as sla_breached,
      -- New business context fields
      business_unit,
      CAST(affected_user_count AS INT) as affected_user_count,
      -- Backward-compatible alias for older API/UI queries
      CAST(affected_user_count AS INT) as user_impact_count,
      affected_roles,
      CAST(productivity_loss_hours AS DOUBLE) as productivity_loss_hours,
      CAST(productivity_loss_usd AS DOUBLE) as productivity_loss_usd,
      CAST(shipments_delayed AS INT) as shipments_delayed,
      CAST(servicenow_ticket_count AS INT) as servicenow_ticket_count,
      CAST(servicenow_duplicate_tickets AS INT) as servicenow_duplicate_tickets,
      downstream_impact_narrative,
      root_cause_explanation,
      revenue_model,
      current_timestamp() as ingested_at
    FROM json.`{VOLUME_PATH}/events/incidents.jsonl`
    """, "Creating bronze_incidents")

    # ── bronze_alerts ──────────────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_alerts AS
    SELECT
      alert_id,
      incident_id,
      service,
      alert_name,
      severity,
      CAST(fired_at AS TIMESTAMP) as fired_at,
      CAST(resolved_at AS TIMESTAMP) as resolved_at,
      CAST(threshold_value AS DOUBLE) as threshold_value,
      CAST(actual_value AS DOUBLE) as actual_value,
      domain,
      environment,
      current_timestamp() as ingested_at
    FROM json.`{VOLUME_PATH}/events/alerts.jsonl`
    """, "Creating bronze_alerts")

    # ── bronze_topology_changes ────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_topology_changes AS
    SELECT
      change_id,
      service,
      change_type,
      description,
      CAST(executed_at AS TIMESTAMP) as executed_at,
      executed_by,
      risk_level,
      CAST(rollback_available AS BOOLEAN) as rollback_available,
      domain,
      environment,
      region,
      current_timestamp() as ingested_at
    FROM json.`{VOLUME_PATH}/events/topology_changes.jsonl`
    """, "Creating bronze_topology_changes")

    # ── bronze_network_flows ───────────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_network_flows (
      flow_id STRING,
      event_timestamp TIMESTAMP,
      src_ip STRING,
      src_port INT,
      dst_ip STRING,
      dst_port INT,
      protocol STRING,
      bytes_sent BIGINT,
      bytes_received BIGINT,
      packets_sent BIGINT,
      packets_received BIGINT,
      latency_us BIGINT,
      retransmits INT,
      src_service STRING,
      dst_service STRING,
      src_zone STRING,
      dst_zone STRING,
      direction STRING,
      connection_reset BOOLEAN,
      timeout BOOLEAN,
      tls_version STRING,
      dns_query STRING,
      dns_response_code INT,
      load_balancer_id STRING,
      environment STRING,
      region STRING,
      ingested_at TIMESTAMP
    )
    """, "Creating bronze_network_flows schema")

    flow_pairs = [
        ("api-gateway", "10.1.0.10", "check-inventory-api", "10.1.1.10", 8080, "dmz", "internal", "ingress", "'apigw-prod-01'"),
        ("check-inventory-api", "10.1.1.10", "erp-sap-connector", "10.1.4.10", 8443, "internal", "data", "internal", "''"),
        ("order-management-service", "10.1.1.11", "check-inventory-api", "10.1.1.10", 8080, "internal", "internal", "internal", "''"),
        ("api-gateway", "10.1.0.10", "sagemaker-inference-endpoint", "10.1.2.20", 8501, "dmz", "ml", "ingress", "'apigw-prod-01'"),
        ("ds-notebook-platform", "10.1.2.21", "sagemaker-inference-endpoint", "10.1.2.20", 8501, "ml", "ml", "internal", "''"),
        ("ctms-api", "10.1.3.10", "edatacapture-service", "10.1.3.11", 8101, "internal", "internal", "internal", "''"),
        ("crm-integration-api", "10.1.4.10", "hcp-portal", "10.1.4.11", 3001, "internal", "internal", "internal", "''"),
        ("auth-service", "10.1.1.16", "identity-provider", "10.1.5.10", 636, "internal", "infra", "internal", "''"),
        ("contract-pricing-api", "10.1.4.12", "rebate-processing-service", "10.1.4.13", 8113, "internal", "internal", "internal", "''"),
        ("adverse-event-reporter", "10.1.3.12", "regulatory-submission-api", "10.1.3.13", 8104, "internal", "internal", "internal", "''"),
    ]

    insert_parts = []
    for src_svc, src_ip, dst_svc, dst_ip, dst_port, src_zone, dst_zone, direction, lb_id in flow_pairs:
        insert_parts.append(f"""
        SELECT
          uuid() as flow_id,
          TIMESTAMP '2025-08-29' + make_interval(0, 0, 0, CAST(floor(rand() * 180) AS INT), CAST(floor(rand()*24) AS INT), CAST(floor(rand()*60) AS INT), CAST(floor(rand()*60) AS INT)) as event_timestamp,
          '{src_ip}', CAST(32768 + floor(rand() * 32767) AS INT), '{dst_ip}', {dst_port},
          (CASE WHEN rand() < 0.5 THEN 'TCP' WHEN rand() < 0.8 THEN 'HTTP' ELSE 'gRPC' END),
          CAST(100 + floor(rand() * 50000) AS BIGINT), CAST(200 + floor(rand() * 100000) AS BIGINT),
          CAST(5 + floor(rand() * 500) AS BIGINT), CAST(5 + floor(rand() * 500) AS BIGINT),
          CAST(500 + floor(rand() * 50000) AS BIGINT), CAST(floor(rand() * 5) AS INT),
          '{src_svc}', '{dst_svc}', '{src_zone}', '{dst_zone}', '{direction}',
          CASE WHEN rand() < 0.02 THEN true ELSE false END,
          CASE WHEN rand() < 0.01 THEN true ELSE false END,
          (CASE WHEN rand() < 0.6 THEN 'TLS1.3' ELSE 'TLS1.2' END),
          '', 0, {lb_id}, 'prod',
          (CASE WHEN rand() < 0.5 THEN 'us-east-1' ELSE 'us-west-2' END),
          current_timestamp()
        FROM range(500)
        """)

    union_sql = " UNION ALL ".join(insert_parts)
    execute_sql(w, warehouse_id, f"""
    INSERT INTO {CATALOG}.{SCHEMA}.bronze_network_flows
    {union_sql}
    """, "Populating bronze_network_flows")

    # Verify counts
    for table in ["bronze_metrics", "bronze_logs", "bronze_traces", "bronze_incidents",
                   "bronze_alerts", "bronze_topology_changes", "bronze_network_flows"]:
        try:
            resp = execute_sql(w, warehouse_id, f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{table}")
            if resp.result and resp.result.data_array:
                count = resp.result.data_array[0][0]
                print(f"  {table}: {count} rows")
        except Exception as e:
            print(f"  {table}: Error - {e}")

    print("\nBronze tables created successfully.")


if __name__ == "__main__":
    main()
