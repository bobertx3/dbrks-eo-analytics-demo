# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Network Flows — Binary .pb -> Delta (Bronze)
# MAGIC
# MAGIC Reads network flow files from `/Volumes/.../network_flows/*.pb`, decodes the
# MAGIC custom binary batch format produced by `data_setup/02_generate_protobuf_network_flows.py`,
# MAGIC and overwrites `bronze_network_flows`.

# COMMAND ----------

import os
import struct
from datetime import datetime, timezone

from pyspark.sql import Row, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    TimestampType,
)

CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"
NETWORK_PATH = f"{VOLUME_PATH}/network_flows"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.bronze_network_flows"

print(f"Source volume : {NETWORK_PATH}")
print(f"Target table  : {TARGET_TABLE}")

# COMMAND ----------

class BinaryDecoder:
    """Decode the custom big-endian batch format used by generated flow .pb files."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def _read(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise EOFError("Unexpected end of file")
        out = self.data[self.pos : self.pos + n]
        self.pos += n
        return out

    def _read_i32(self) -> int:
        return struct.unpack(">i", self._read(4))[0]

    def _read_i64(self) -> int:
        return struct.unpack(">q", self._read(8))[0]

    def _read_bool(self) -> bool:
        return struct.unpack(">?", self._read(1))[0]

    def _read_str(self) -> str:
        n = struct.unpack(">H", self._read(2))[0]
        return self._read(n).decode("utf-8")


def decode_flow_record(data: bytes) -> dict:
    d = BinaryDecoder(data)
    return {
        "flow_id": d._read_str(),
        "timestamp_unix_nano": d._read_i64(),
        "src_ip": d._read_str(),
        "src_port": d._read_i32(),
        "dst_ip": d._read_str(),
        "dst_port": d._read_i32(),
        "protocol": d._read_str(),
        "bytes_sent": d._read_i64(),
        "bytes_received": d._read_i64(),
        "packets_sent": d._read_i64(),
        "packets_received": d._read_i64(),
        "latency_us": d._read_i64(),
        "retransmits": d._read_i32(),
        "src_service": d._read_str(),
        "dst_service": d._read_str(),
        "src_zone": d._read_str(),
        "dst_zone": d._read_str(),
        "direction": d._read_str(),
        "connection_reset": d._read_bool(),
        "timeout": d._read_bool(),
        "tls_version": d._read_str(),
        "dns_query": d._read_str(),
        "dns_response_code": d._read_i32(),
        "load_balancer_id": d._read_str(),
        "environment": d._read_str(),
        "region": d._read_str(),
    }


def decode_batch(raw: bytes) -> list[dict]:
    d = BinaryDecoder(raw)
    magic = d._read(5)
    if magic != b"NFLOW":
        raise ValueError("Invalid network flow file header")

    _version = d._read_i32()
    record_count = d._read_i32()
    _export_ts = d._read_str()
    _collector_id = d._read_str()

    records = []
    for _ in range(record_count):
        rec_len = d._read_i32()
        rec_bytes = d._read(rec_len)
        records.append(decode_flow_record(rec_bytes))
    return records


# COMMAND ----------

pb_files = [e.path for e in dbutils.fs.ls(NETWORK_PATH) if e.name.endswith(".pb")]

print(f"Found {len(pb_files)} network flow files in {NETWORK_PATH}")
for f in pb_files:
    print(f"  {f.split('/')[-1]}")

if not pb_files:
    raise Exception(
        f"No .pb files found at {NETWORK_PATH}. Run data_setup/02_generate_protobuf_network_flows.py first."
    )

# COMMAND ----------

schema = StructType([
    StructField("flow_id", StringType()),
    StructField("event_timestamp", TimestampType()),
    StructField("src_ip", StringType()),
    StructField("src_port", IntegerType()),
    StructField("dst_ip", StringType()),
    StructField("dst_port", IntegerType()),
    StructField("protocol", StringType()),
    StructField("bytes_sent", LongType()),
    StructField("bytes_received", LongType()),
    StructField("packets_sent", LongType()),
    StructField("packets_received", LongType()),
    StructField("latency_us", LongType()),
    StructField("retransmits", IntegerType()),
    StructField("src_service", StringType()),
    StructField("dst_service", StringType()),
    StructField("src_zone", StringType()),
    StructField("dst_zone", StringType()),
    StructField("direction", StringType()),
    StructField("connection_reset", BooleanType()),
    StructField("timeout", BooleanType()),
    StructField("tls_version", StringType()),
    StructField("dns_query", StringType()),
    StructField("dns_response_code", IntegerType()),
    StructField("load_balancer_id", StringType()),
    StructField("environment", StringType()),
    StructField("region", StringType()),
])

rows = []

for pb_file in pb_files:
    local_path = pb_file.replace("dbfs:", "")
    with open(local_path, "rb") as f:
        raw = f.read()

    decoded = decode_batch(raw)
    for rec in decoded:
        ts = datetime.fromtimestamp(rec["timestamp_unix_nano"] / 1e9, tz=timezone.utc).replace(tzinfo=None)
        rows.append(
            Row(
                flow_id=rec["flow_id"],
                event_timestamp=ts,
                src_ip=rec["src_ip"],
                src_port=rec["src_port"],
                dst_ip=rec["dst_ip"],
                dst_port=rec["dst_port"],
                protocol=rec["protocol"],
                bytes_sent=rec["bytes_sent"],
                bytes_received=rec["bytes_received"],
                packets_sent=rec["packets_sent"],
                packets_received=rec["packets_received"],
                latency_us=rec["latency_us"],
                retransmits=rec["retransmits"],
                src_service=rec["src_service"],
                dst_service=rec["dst_service"],
                src_zone=rec["src_zone"],
                dst_zone=rec["dst_zone"],
                direction=rec["direction"],
                connection_reset=rec["connection_reset"],
                timeout=rec["timeout"],
                tls_version=rec["tls_version"],
                dns_query=rec["dns_query"],
                dns_response_code=rec["dns_response_code"],
                load_balancer_id=rec["load_balancer_id"],
                environment=rec["environment"],
                region=rec["region"],
            )
        )

print(f"Decoded {len(rows):,} network flow records")

# COMMAND ----------

df = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())

print(f"Writing {df.count():,} rows to {TARGET_TABLE} ...")

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

print(f"Done. bronze_network_flows now contains {spark.table(TARGET_TABLE).count():,} rows.")
