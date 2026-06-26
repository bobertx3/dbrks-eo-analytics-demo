# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest OTLP Metrics — Protobuf → Delta (Bronze)
# MAGIC
# MAGIC This notebook reads binary `.pb` files from the Unity Catalog raw landing volume,
# MAGIC decodes them using the **OpenTelemetry Protocol (OTLP) metrics schema**, and writes
# MAGIC flattened rows to the `bronze_metrics` Delta table.
# MAGIC
# MAGIC ## Protobuf Schema
# MAGIC The binary files conform to the OTLP `MetricsData` message defined in:
# MAGIC ```
# MAGIC /Volumes/bldemos/eo_analytics/raw_landing/metrics/otlp_metrics.proto
# MAGIC ```
# MAGIC Key message hierarchy:
# MAGIC ```
# MAGIC MetricsData
# MAGIC   └── ResourceMetrics[]          ← one per service/host
# MAGIC         ├── Resource.attributes  ← service.name, environment, region, host.name
# MAGIC         └── ScopeMetrics[]
# MAGIC               └── Metric[]       ← gauge | sum | histogram
# MAGIC                     └── DataPoints[]
# MAGIC                           ├── time_unix_nano
# MAGIC                           └── value (as_double | as_int | count+sum)
# MAGIC ```
# MAGIC
# MAGIC > **Note:** Rather than running `protoc` to compile the `.proto` file at runtime,
# MAGIC > this notebook implements a lightweight wire-format decoder in pure Python.
# MAGIC > This approach has zero external dependencies and works on any Databricks runtime.
# MAGIC > The field numbers used (1, 2, 5, 7, 9, etc.) map directly to the `.proto` field
# MAGIC > definitions — see `data_setup/otlp_metrics.proto` for the authoritative schema reference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Configuration

# COMMAND ----------

import os
import re
import struct
from datetime import datetime, timezone
from pyspark.sql import Row, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA       = "eo_analytics"
VOLUME_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"
METRICS_PATH = f"{VOLUME_PATH}/metrics"
PROTO_PATH   = f"{METRICS_PATH}/otlp_metrics.proto"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_metrics"

print(f"Source volume : {METRICS_PATH}")
print(f"Target table  : {TARGET_TABLE}")
print(f"Proto schema  : {PROTO_PATH}")

# COMMAND ----------

def parse_proto_field_numbers(proto_text: str) -> dict:
    """Parse top-level message field numbers from a proto3 file."""
    proto_text = re.sub(r"//.*", "", proto_text)
    message_pat = re.compile(r"message\s+(\w+)\s*\{(.*?)\}", re.S)
    field_pat = re.compile(r"\b(?:repeated\s+)?(?:\w+\.)?\w+\s+(\w+)\s*=\s*(\d+)\s*;")

    fields_by_message = {}
    for message_name, body in message_pat.findall(proto_text):
        fields = {}
        for field_name, field_num in field_pat.findall(body):
            fields[field_name] = int(field_num)
        if fields:
            fields_by_message[message_name] = fields
    return fields_by_message


def field_num(fields_by_message: dict, message_name: str, field_name: str) -> int:
    try:
        return fields_by_message[message_name][field_name]
    except KeyError as exc:
        raise ValueError(
            f"Missing field mapping for {message_name}.{field_name} in {PROTO_PATH}"
        ) from exc


with open(PROTO_PATH, "r", encoding="utf-8") as f:
    proto_fields = parse_proto_field_numbers(f.read())

FIELD_MAP = {
    "METRICSDATA_RESOURCE_METRICS": field_num(proto_fields, "MetricsData", "resource_metrics"),
    "RESOURCEMETRICS_RESOURCE": field_num(proto_fields, "ResourceMetrics", "resource"),
    "RESOURCEMETRICS_SCOPE_METRICS": field_num(proto_fields, "ResourceMetrics", "scope_metrics"),
    "RESOURCE_ATTRIBUTES": field_num(proto_fields, "Resource", "attributes"),
    "SCOPEMETRICS_SCOPE": field_num(proto_fields, "ScopeMetrics", "scope"),
    "SCOPEMETRICS_METRICS": field_num(proto_fields, "ScopeMetrics", "metrics"),
    "INSTRUMENTATIONSCOPE_NAME": field_num(proto_fields, "InstrumentationScope", "name"),
    "METRIC_NAME": field_num(proto_fields, "Metric", "name"),
    "METRIC_UNIT": field_num(proto_fields, "Metric", "unit"),
    "METRIC_GAUGE": field_num(proto_fields, "Metric", "gauge"),
    "METRIC_SUM": field_num(proto_fields, "Metric", "sum"),
    "METRIC_HISTOGRAM": field_num(proto_fields, "Metric", "histogram"),
    "GAUGE_DATA_POINTS": field_num(proto_fields, "Gauge", "data_points"),
    "SUM_DATA_POINTS": field_num(proto_fields, "Sum", "data_points"),
    "HISTOGRAM_DATA_POINTS": field_num(proto_fields, "Histogram", "data_points"),
    "NUMBERDATAPOINT_ATTRIBUTES": field_num(proto_fields, "NumberDataPoint", "attributes"),
    "NUMBERDATAPOINT_TIME_UNIX_NANO": field_num(proto_fields, "NumberDataPoint", "time_unix_nano"),
    "NUMBERDATAPOINT_AS_DOUBLE": field_num(proto_fields, "NumberDataPoint", "as_double"),
    "NUMBERDATAPOINT_AS_INT": field_num(proto_fields, "NumberDataPoint", "as_int"),
    "HISTOGRAMDATAPOINT_ATTRIBUTES": field_num(proto_fields, "HistogramDataPoint", "attributes"),
    "HISTOGRAMDATAPOINT_TIME_UNIX_NANO": field_num(proto_fields, "HistogramDataPoint", "time_unix_nano"),
    "HISTOGRAMDATAPOINT_COUNT": field_num(proto_fields, "HistogramDataPoint", "count"),
    "HISTOGRAMDATAPOINT_SUM": field_num(proto_fields, "HistogramDataPoint", "sum"),
    "KEYVALUE_KEY": field_num(proto_fields, "KeyValue", "key"),
    "KEYVALUE_VALUE": field_num(proto_fields, "KeyValue", "value"),
    "ANYVALUE_STRING_VALUE": field_num(proto_fields, "AnyValue", "string_value"),
    "ANYVALUE_BOOL_VALUE": field_num(proto_fields, "AnyValue", "bool_value"),
    "ANYVALUE_INT_VALUE": field_num(proto_fields, "AnyValue", "int_value"),
    "ANYVALUE_DOUBLE_VALUE": field_num(proto_fields, "AnyValue", "double_value"),
}

print(f"Loaded .proto field mapping from {PROTO_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Protobuf Wire-Format Decoder
# MAGIC
# MAGIC Implements a minimal decoder for the OTLP `MetricsData` message.
# MAGIC
# MAGIC Protobuf wire types used here:
# MAGIC | Wire Type | Encoding | Used for |
# MAGIC |-----------|----------|----------|
# MAGIC | `0` | Varint | int32, int64, bool, enum |
# MAGIC | `1` | 64-bit | fixed64, double |
# MAGIC | `2` | Length-delimited | string, bytes, embedded messages |
# MAGIC | `5` | 32-bit | float, fixed32 |
# MAGIC
# MAGIC Field numbers map directly to `data_setup/otlp_metrics.proto` — e.g. `field 1` in `Metric`
# MAGIC is `name` (string), `field 5` is `gauge` (embedded Gauge message), etc.

# COMMAND ----------

class ProtobufDecoder:
    """Minimal protobuf wire-format decoder. Field numbers match data_setup/otlp_metrics.proto."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos  = 0

    def _read_varint(self) -> int:
        result, shift = 0, 0
        while True:
            if self.pos >= len(self.data):
                raise EOFError("Unexpected end of protobuf data")
            b = self.data[self.pos]; self.pos += 1
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        return result

    def _read_bytes(self, n: int) -> bytes:
        val = self.data[self.pos : self.pos + n]
        self.pos += n
        return val

    def read_all_fields(self):
        fields = []
        while self.pos < len(self.data):
            tag = self._read_varint()
            fn, wt = tag >> 3, tag & 0x07
            if   wt == 0: val = self._read_varint()
            elif wt == 1: val = self._read_bytes(8)
            elif wt == 2: val = self._read_bytes(self._read_varint())
            elif wt == 5: val = self._read_bytes(4)
            else: break
            fields.append((fn, wt, val))
        return fields


# ── Helpers mapping proto field numbers → Python values ──────────────────────

def _str(b: bytes) -> str:
    return b.decode("utf-8")

def _kv(data: bytes):
    """KeyValue { key, value }"""
    dec = ProtobufDecoder(data)
    key, val = "", {}
    for fn, wt, v in dec.read_all_fields():
        if fn == FIELD_MAP["KEYVALUE_KEY"] and wt == 2:
            key = _str(v)
        elif fn == FIELD_MAP["KEYVALUE_VALUE"] and wt == 2:
            val = _any_value(v)
    return key, val

def _any_value(data: bytes) -> dict:
    """AnyValue — returns first populated oneof"""
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["ANYVALUE_STRING_VALUE"] and wt == 2:
            return {"string_value": _str(v)}
        elif fn == FIELD_MAP["ANYVALUE_BOOL_VALUE"] and wt == 0:
            return {"bool_value": bool(v)}
        elif fn == FIELD_MAP["ANYVALUE_INT_VALUE"] and wt == 0:
            return {"int_value": v}
        elif fn == FIELD_MAP["ANYVALUE_DOUBLE_VALUE"] and wt == 1:
            return {"double_value": struct.unpack("<d", v)[0]}
    return {}

def _number_dp(data: bytes) -> dict:
    """NumberDataPoint { attributes, time_unix_nano, as_double, as_int }"""
    dp = {"attributes": [], "time_unix_nano": 0}
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["NUMBERDATAPOINT_ATTRIBUTES"] and wt == 2:
            dp["attributes"].append(_kv(v))
        elif fn == FIELD_MAP["NUMBERDATAPOINT_TIME_UNIX_NANO"] and wt == 1:
            dp["time_unix_nano"] = struct.unpack("<Q", v)[0]
        elif fn == FIELD_MAP["NUMBERDATAPOINT_AS_DOUBLE"] and wt == 1:
            dp["as_double"] = struct.unpack("<d", v)[0]
        elif fn == FIELD_MAP["NUMBERDATAPOINT_AS_INT"] and wt == 1:
            dp["as_int"] = struct.unpack("<q", v)[0]
    return dp

def _histogram_dp(data: bytes) -> dict:
    """HistogramDataPoint { attributes, time_unix_nano, count, sum }"""
    dp = {"attributes": [], "time_unix_nano": 0, "count": 0, "sum": 0.0}
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["HISTOGRAMDATAPOINT_ATTRIBUTES"] and wt == 2:
            dp["attributes"].append(_kv(v))
        elif fn == FIELD_MAP["HISTOGRAMDATAPOINT_TIME_UNIX_NANO"] and wt == 1:
            dp["time_unix_nano"] = struct.unpack("<Q", v)[0]
        elif fn == FIELD_MAP["HISTOGRAMDATAPOINT_COUNT"] and wt == 0:
            dp["count"] = v
        elif fn == FIELD_MAP["HISTOGRAMDATAPOINT_SUM"] and wt == 1:
            dp["sum"] = struct.unpack("<d", v)[0]
    return dp

def _metric(data: bytes) -> dict:
    """Metric { name, unit, gauge|sum|histogram }"""
    m = {"name": "", "unit": "", "type": "unknown", "data_points": []}
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["METRIC_NAME"] and wt == 2:
            m["name"] = _str(v)
        elif fn == FIELD_MAP["METRIC_UNIT"] and wt == 2:
            m["unit"] = _str(v)
        elif fn == FIELD_MAP["METRIC_GAUGE"] and wt == 2:  # Gauge
            m["type"] = "gauge"
            for gfn, gwt, gv in ProtobufDecoder(v).read_all_fields():
                if gfn == FIELD_MAP["GAUGE_DATA_POINTS"] and gwt == 2:
                    m["data_points"].append(_number_dp(gv))
        elif fn == FIELD_MAP["METRIC_SUM"] and wt == 2:  # Sum (counter)
            m["type"] = "sum"
            for sfn, swt, sv in ProtobufDecoder(v).read_all_fields():
                if sfn == FIELD_MAP["SUM_DATA_POINTS"] and swt == 2:
                    m["data_points"].append(_number_dp(sv))
        elif fn == FIELD_MAP["METRIC_HISTOGRAM"] and wt == 2:  # Histogram
            m["type"] = "histogram"
            for hfn, hwt, hv in ProtobufDecoder(v).read_all_fields():
                if hfn == FIELD_MAP["HISTOGRAM_DATA_POINTS"] and hwt == 2:
                    m["data_points"].append(_histogram_dp(hv))
    return m

def _scope_metrics(data: bytes):
    """ScopeMetrics { scope(name), metrics }"""
    scope_name, metrics = "", []
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["SCOPEMETRICS_SCOPE"] and wt == 2:
            for sfn, swt, sv in ProtobufDecoder(v).read_all_fields():
                if sfn == FIELD_MAP["INSTRUMENTATIONSCOPE_NAME"] and swt == 2:
                    scope_name = _str(sv)
        elif fn == FIELD_MAP["SCOPEMETRICS_METRICS"] and wt == 2:
            metrics.append(_metric(v))
    return scope_name, metrics

def _resource(data: bytes) -> dict:
    """Resource { attributes (KeyValue[]) }"""
    attrs = {}
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["RESOURCE_ATTRIBUTES"] and wt == 2:
            k, val = _kv(v)
            attrs[k] = val.get("string_value", str(next(iter(val.values()), "")))
    return attrs

def decode_metrics_file(data: bytes):
    """
    Top-level decoder: MetricsData { resource_metrics (ResourceMetrics[]) }
    Returns list of (resource_attrs_dict, [(scope_name, [metric_dict])])
    """
    result = []
    for fn, wt, v in ProtobufDecoder(data).read_all_fields():
        if fn == FIELD_MAP["METRICSDATA_RESOURCE_METRICS"] and wt == 2:
            resource_attrs, scope_list = {}, []
            for rfn, rwt, rv in ProtobufDecoder(v).read_all_fields():
                if rfn == FIELD_MAP["RESOURCEMETRICS_RESOURCE"] and rwt == 2:
                    resource_attrs = _resource(rv)
                elif rfn == FIELD_MAP["RESOURCEMETRICS_SCOPE_METRICS"] and rwt == 2:
                    scope_list.append(_scope_metrics(rv))
            result.append((resource_attrs, scope_list))
    return result

print(f"Protobuf decoder loaded — field numbers sourced from {PROTO_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Discover `.pb` Files in Volume

# COMMAND ----------

pb_files = [
    e.path for e in dbutils.fs.ls(METRICS_PATH)
    if e.name.endswith(".pb")
]

print(f"Found {len(pb_files)} protobuf metric files in {METRICS_PATH}")
print()
for f in pb_files:
    print(f"  {f.split('/')[-1]}")

if not pb_files:
    raise Exception(f"No .pb files found at {METRICS_PATH}. Run data_setup/01_generate_raw_telemetry.py first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Decode Each File and Flatten to Rows

# COMMAND ----------

schema = StructType([
    StructField("service_name",       StringType()),
    StructField("environment",        StringType()),
    StructField("region",             StringType()),
    StructField("host_name",          StringType()),
    StructField("service_version",    StringType()),
    StructField("scope_name",         StringType()),
    StructField("metric_name",        StringType()),
    StructField("metric_unit",        StringType()),
    StructField("metric_value",       DoubleType()),
    StructField("histogram_count",    LongType()),
    StructField("histogram_sum",      DoubleType()),
    StructField("metric_type",        StringType()),
    StructField("timestamp_unix_nano",LongType()),
    StructField("event_timestamp",    TimestampType()),
])

all_rows     = []
file_summary = []

for pb_file in pb_files:
    # UC volumes are at /Volumes/... — strip the dbfs: prefix that dbutils.fs.ls adds
    local_path = pb_file.replace("dbfs:", "")
    filename   = local_path.split("/")[-1]

    with open(local_path, "rb") as f:
        raw_bytes = f.read()

    file_size_kb = len(raw_bytes) / 1024
    decoded      = decode_metrics_file(raw_bytes)
    rows_before  = len(all_rows)

    for resource_attrs, scope_entries in decoded:
        svc  = resource_attrs.get("service.name",          "")
        env  = resource_attrs.get("deployment.environment","")
        reg  = resource_attrs.get("cloud.region",          "")
        host = resource_attrs.get("host.name",             "")
        ver  = resource_attrs.get("service.version",       "")

        for scope_name, metrics in scope_entries:
            for metric in metrics:
                for dp in metric["data_points"]:
                    tnano = dp.get("time_unix_nano", 0)
                    ts    = datetime.fromtimestamp(tnano / 1e9, tz=timezone.utc).replace(tzinfo=None) if tnano else None

                    if metric["type"] == "histogram":
                        mval   = None
                        hcount = dp.get("count", 0)
                        hsum   = dp.get("sum",   0.0)
                    else:
                        mval   = dp.get("as_double") or (float(dp["as_int"]) if "as_int" in dp else None)
                        hcount = None
                        hsum   = None

                    all_rows.append(Row(
                        service_name=svc, environment=env, region=reg,
                        host_name=host, service_version=ver, scope_name=scope_name,
                        metric_name=metric["name"], metric_unit=metric["unit"],
                        metric_value=mval, histogram_count=hcount, histogram_sum=hsum,
                        metric_type=metric["type"], timestamp_unix_nano=tnano,
                        event_timestamp=ts,
                    ))

    rows_this_file = len(all_rows) - rows_before
    file_summary.append((filename, f"{file_size_kb:.1f} KB", rows_this_file, svc))
    print(f"  {filename:<40}  {file_size_kb:>7.1f} KB  →  {rows_this_file:>6,} data points  (service: {svc})")

print(f"\nTotal decoded: {len(all_rows):,} metric data points across {len(pb_files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Write to `bronze_metrics` Delta Table

# COMMAND ----------

df = spark.createDataFrame(all_rows, schema) \
          .withColumn("ingested_at", F.current_timestamp())

print(f"Writing {df.count():,} rows to {TARGET_TABLE} ...")

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"Done. bronze_metrics now contains {spark.table(TARGET_TABLE).count():,} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Verify: Sample Records

# COMMAND ----------

print("Sample records from bronze_metrics:")
spark.table(TARGET_TABLE) \
    .select("service_name", "metric_name", "metric_type", "metric_value", "histogram_count", "event_timestamp") \
    .orderBy(F.rand()) \
    .limit(10) \
    .show(truncate=False)

print("\nMetric types breakdown:")
spark.table(TARGET_TABLE) \
    .groupBy("metric_type") \
    .count() \
    .show()

print("\nServices ingested:")
spark.table(TARGET_TABLE) \
    .groupBy("service_name") \
    .agg(F.count("*").alias("data_points"), F.countDistinct("metric_name").alias("distinct_metrics")) \
    .orderBy(F.desc("data_points")) \
    .show(truncate=False)
