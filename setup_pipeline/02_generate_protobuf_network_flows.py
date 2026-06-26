"""
data_setup/02_generate_protobuf_network_flows.py
Generates network flow telemetry as Protocol Buffer binary files.
Uses a simple binary serialization that mirrors the .proto schema.
Also uploads the .proto schema file to the volume.

Output: 10 .pb files, one per service-pair group.
"""
import json
import random
import uuid
import struct
import os
import io
from datetime import datetime, timedelta, timezone
from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"

random.seed(42)


def network_flows_volume_has_data(w):
    """Return True if the network_flows subdir already contains any data files (not schema .proto)."""
    try:
        entries = list(w.files.list_directory_contents(f"{VOLUME_PATH}/network_flows"))
    except Exception:
        return False
    for e in entries:
        path = getattr(e, "path", None) or ""
        name = path.rstrip("/").split("/")[-1]
        if not name:
            continue
        if name.endswith(".proto"):
            continue
        if name.endswith(".pb"):
            return True
    return False

# Service IP mapping
SERVICE_IPS = {
    "ehr-api": "10.1.1.10",
    "patient-portal": "10.1.1.11",
    "clinical-decision-support": "10.1.1.12",
    "pharmacy-service": "10.1.1.13",
    "imaging-service": "10.1.1.14",
    "hl7-gateway": "10.1.2.10",
    "fhir-api": "10.1.1.15",
    "auth-service": "10.1.1.16",
    "notification-service": "10.1.1.17",
    "ml-inference-service": "10.1.3.10",
    "ehr-database": "10.1.4.10",
    "auth-database": "10.1.4.11",
    "drug-interaction-db": "10.1.4.12",
    "message-queue": "10.1.5.10",
    "pacs-storage": "10.1.6.10",
    "dicom-gateway": "10.1.2.11",
    "load-balancer": "10.1.0.10",
    "dns-resolver": "10.1.0.11",
    "vpn-gateway": "10.1.0.12",
    "terminology-service": "10.1.1.18",
}

SERVICE_PORTS = {
    "ehr-api": 8080, "patient-portal": 3000, "clinical-decision-support": 8081,
    "pharmacy-service": 8082, "imaging-service": 8083, "hl7-gateway": 2575,
    "fhir-api": 8084, "auth-service": 8085, "notification-service": 8086,
    "ml-inference-service": 8087, "ehr-database": 5432, "auth-database": 5432,
    "drug-interaction-db": 5432, "message-queue": 5672, "pacs-storage": 11112,
    "dicom-gateway": 4242, "load-balancer": 443, "dns-resolver": 53,
    "vpn-gateway": 1194, "terminology-service": 8088,
}

ZONES = {
    "load-balancer": "dmz", "dns-resolver": "dmz", "vpn-gateway": "dmz",
    "hl7-gateway": "dmz", "dicom-gateway": "dmz",
    "ehr-database": "data", "auth-database": "data", "drug-interaction-db": "data",
    "pacs-storage": "data", "message-queue": "data",
}

# Flow patterns grouped into 10 batches (one file per batch)
FLOW_PATTERN_GROUPS = [
    [("load-balancer", "patient-portal"), ("load-balancer", "ehr-api")],
    [("load-balancer", "fhir-api"), ("patient-portal", "ehr-api")],
    [("patient-portal", "auth-service"), ("ehr-api", "ehr-database")],
    [("ehr-api", "auth-service"), ("ehr-api", "fhir-api")],
    [("fhir-api", "ehr-database"), ("fhir-api", "terminology-service")],
    [("clinical-decision-support", "ehr-api"), ("clinical-decision-support", "ml-inference-service")],
    [("pharmacy-service", "ehr-api"), ("pharmacy-service", "drug-interaction-db")],
    [("pharmacy-service", "hl7-gateway"), ("imaging-service", "pacs-storage")],
    [("imaging-service", "dicom-gateway"), ("hl7-gateway", "message-queue")],
    [("hl7-gateway", "ehr-database"), ("notification-service", "message-queue"), ("auth-service", "auth-database"), ("terminology-service", "ehr-database")],
]


def encode_string(s):
    """Length-prefixed UTF-8 string encoding."""
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b


def encode_flow_record(rec):
    """Encode a single network flow record as binary."""
    data = b""
    data += encode_string(rec["flow_id"])
    data += struct.pack(">q", rec["timestamp_unix_nano"])
    data += encode_string(rec["src_ip"])
    data += struct.pack(">i", rec["src_port"])
    data += encode_string(rec["dst_ip"])
    data += struct.pack(">i", rec["dst_port"])
    data += encode_string(rec["protocol"])
    data += struct.pack(">q", rec["bytes_sent"])
    data += struct.pack(">q", rec["bytes_received"])
    data += struct.pack(">q", rec["packets_sent"])
    data += struct.pack(">q", rec["packets_received"])
    data += struct.pack(">q", rec["latency_us"])
    data += struct.pack(">i", rec["retransmits"])
    data += encode_string(rec["src_service"])
    data += encode_string(rec["dst_service"])
    data += encode_string(rec["src_zone"])
    data += encode_string(rec["dst_zone"])
    data += encode_string(rec["direction"])
    data += struct.pack(">?", rec["connection_reset"])
    data += struct.pack(">?", rec["timeout"])
    data += encode_string(rec["tls_version"])
    data += encode_string(rec.get("dns_query", ""))
    data += struct.pack(">i", rec.get("dns_response_code", 0))
    data += encode_string(rec.get("load_balancer_id", ""))
    data += encode_string(rec["environment"])
    data += encode_string(rec["region"])
    return data


def encode_batch(records, export_ts, collector_id):
    """Encode a batch of flow records with header."""
    header = b"NFLOW"
    header += struct.pack(">i", 1)
    header += struct.pack(">i", len(records))
    header += encode_string(export_ts)
    header += encode_string(collector_id)

    data = header
    for rec in records:
        encoded = encode_flow_record(rec)
        data += struct.pack(">i", len(encoded))
        data += encoded
    return data


def ts_to_unix_nano(dt):
    return int(dt.timestamp() * 1_000_000_000)


def generate_flow_record(src_svc, dst_svc, ts, is_anomalous=False):
    """Generate a single network flow record."""
    src_ip = SERVICE_IPS.get(src_svc, "10.1.9.99")
    dst_ip = SERVICE_IPS.get(dst_svc, "10.1.9.99")
    src_port = random.randint(32768, 65535)
    dst_port = SERVICE_PORTS.get(dst_svc, 8080)

    base_latency = random.randint(500, 5000)
    if is_anomalous:
        base_latency *= random.randint(10, 100)

    return {
        "flow_id": uuid.uuid4().hex[:16],
        "timestamp_unix_nano": ts_to_unix_nano(ts),
        "src_ip": src_ip,
        "src_port": src_port,
        "dst_ip": dst_ip,
        "dst_port": dst_port,
        "protocol": random.choice(["TCP", "TCP", "HTTP", "gRPC"]),
        "bytes_sent": random.randint(100, 50000) * (5 if is_anomalous else 1),
        "bytes_received": random.randint(200, 100000),
        "packets_sent": random.randint(5, 500),
        "packets_received": random.randint(5, 500),
        "latency_us": base_latency,
        "retransmits": random.randint(5, 50) if is_anomalous else random.randint(0, 2),
        "src_service": src_svc,
        "dst_service": dst_svc,
        "src_zone": ZONES.get(src_svc, "internal"),
        "dst_zone": ZONES.get(dst_svc, "internal"),
        "direction": "ingress" if src_svc == "load-balancer" else "internal",
        "connection_reset": is_anomalous and random.random() < 0.3,
        "timeout": is_anomalous and random.random() < 0.2,
        "tls_version": random.choice(["TLS1.2", "TLS1.3", "TLS1.3"]),
        "dns_query": f"{dst_svc}.internal.hls.net" if random.random() < 0.1 else "",
        "dns_response_code": 0,
        "load_balancer_id": "lb-prod-01" if src_svc == "load-balancer" else "",
        "environment": "prod",
        "region": random.choice(["us-east-1", "us-west-2"]),
    }


def main():
    w = WorkspaceClient(profile=PROFILE)
    if network_flows_volume_has_data(w):
        print(f"Volume {VOLUME_PATH}/network_flows already contains data. Skipping generation to avoid overwriting.")
        print("Delete or clear the network_flows subdir if you want to regenerate.")
        return

    end_date = datetime(2026, 2, 25, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=30)

    print("Generating protobuf network flow data (10 files) ...")
    print(f"  Period: {start_date.date()} to {end_date.date()}")

    # Upload .proto schema file
    proto_path = os.path.join(os.path.dirname(__file__), "network_flow.proto")
    with open(proto_path, "rb") as f:
        w.files.upload(f"{VOLUME_PATH}/network_flows/network_flow.proto", f, overwrite=True)
    print("  Uploaded network_flow.proto schema")

    total_records = 0

    for group_idx, flow_pairs in enumerate(FLOW_PATTERN_GROUPS):
        group_records = []

        current = start_date
        while current < end_date:
            for hour in range(24):
                ts_base = current.replace(hour=hour, minute=0, second=0)

                # Determine if any failure patterns are active
                is_anomalous_hour = False
                for fp in [
                    {"trigger_day": 0, "hour_range": (7, 9), "prob": 0.85},
                    {"trigger_day": None, "hour_range": (14, 16), "prob": 0.40},
                    {"trigger_day": None, "hour_range": (0, 23), "prob": 0.08},
                ]:
                    if fp["trigger_day"] is None or current.weekday() == fp["trigger_day"]:
                        h_lo, h_hi = fp["hour_range"]
                        if h_lo <= hour <= h_hi and random.random() < fp["prob"]:
                            is_anomalous_hour = True

                for src, dst in flow_pairs:
                    num_flows = random.randint(3, 15)
                    for i in range(num_flows):
                        ts = ts_base + timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))
                        record = generate_flow_record(src, dst, ts, is_anomalous=is_anomalous_hour)
                        group_records.append(record)

            current += timedelta(days=1)

        # Encode as protobuf-style binary batch
        pair_names = "_".join(f"{s}-{d}" for s, d in flow_pairs[:2])
        batch_data = encode_batch(
            group_records,
            start_date.isoformat(),
            f"flow-collector-{random.choice(['east', 'west'])}-01"
        )

        file_path = f"{VOLUME_PATH}/network_flows/flows_group_{group_idx:02d}.pb"
        buf = io.BytesIO(batch_data)
        w.files.upload(file_path, buf, overwrite=True)

        total_records += len(group_records)
        print(f"  Written flows_group_{group_idx:02d}.pb ({len(group_records):,} records, pairs: {[f'{s}->{d}' for s,d in flow_pairs]})")

    print(f"  Network flows complete: {len(FLOW_PATTERN_GROUPS)} files, {total_records:,} total records.")


if __name__ == "__main__":
    main()
