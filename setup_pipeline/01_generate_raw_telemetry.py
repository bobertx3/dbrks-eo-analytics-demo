"""
data_setup/01_generate_raw_telemetry.py
Generates realistic Dbrks-style OpenTelemetry data organized by business unit:
  - OTLP Metrics (Protobuf .pb) -- 15 files, one per service batch
  - Structured Logs (JSONL) -- 12 files, ~15 days per file
  - Distributed Traces (JSON) -- 10 files, grouped by service cluster
  - Incident & Alert Events (JSONL) -- 3 single files

Data covers 30 days with rich failure patterns including two anchor stories:
  Story 1: Supply chain disruption (check-inventory-api -> ERP timeout)
  Story 2: Digital surgery productivity loss (SageMaker VPC misconfiguration)

Metrics are serialized using protobuf wire format matching otlp_metrics.proto.
"""
import json
import random
import struct
import uuid
import os
import io
from datetime import datetime, timedelta, timezone
from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"


def volume_subdir_has_data(w, subdir_path, data_extensions=(".pb", ".jsonl", ".json")):
    """Return True if the volume subdirectory contains any data files (not schema .proto)."""
    try:
        entries = list(w.files.list_directory_contents(subdir_path))
    except Exception:
        return False
    for e in entries:
        path = getattr(e, "path", None) or ""
        name = path.rstrip("/").split("/")[-1]
        if not name:
            continue
        if name.endswith(".proto"):
            continue
        if any(name.endswith(ext) for ext in data_extensions):
            return True
    return False


def raw_volume_has_data(w):
    """Return True if any raw telemetry subdir (metrics, logs, traces, events) already has data."""
    for subdir in ("metrics", "logs", "traces", "events"):
        if volume_subdir_has_data(w, f"{VOLUME_PATH}/{subdir}"):
            return True
    return False


# ============================================================================
# BUSINESS UNITS & SERVICE CATALOG  (Dbrks-style)
# ============================================================================

BUSINESS_UNITS = {
    "supply-chain": {
        "services": [
            "check-inventory-api", "order-management-service",
            "shipment-routing-service", "distribution-portal",
            "3pl-integration-api", "erp-sap-connector",
        ],
        "revenue_model": "shipment_throughput",
        "avg_order_value_usd": 12500,
    },
    "digital-surgery": {
        "services": [
            "sagemaker-inference-endpoint", "ml-training-pipeline",
            "ds-notebook-platform", "feature-store-api",
            "model-registry-service", "surgical-robotics-api",
        ],
        "revenue_model": "productivity_loss",
        "loaded_rate_per_hour_usd": 1500,
    },
    "clinical-trials": {
        "services": [
            "ctms-api", "edatacapture-service", "randomization-service",
            "adverse-event-reporter", "regulatory-submission-api",
        ],
        "revenue_model": "trial_delay",
        "trial_delay_cost_per_day_usd": 350000,
    },
    "commercial-pharma": {
        "services": [
            "crm-integration-api", "hcp-portal", "sample-management-service",
            "contract-pricing-api", "rebate-processing-service",
        ],
        "revenue_model": "lost_sales",
        "revenue_per_hour_usd": 95000,
    },
    "shared-infrastructure": {
        "services": [
            "auth-service", "api-gateway", "data-mesh-platform",
            "identity-provider", "secrets-manager", "dns-resolver",
        ],
        "revenue_model": "blast_multiplier",
    },
}

# Build flat service -> business_unit lookup
SERVICE_TO_BU = {}
for bu_name, bu_info in BUSINESS_UNITS.items():
    for svc in bu_info["services"]:
        SERVICE_TO_BU[svc] = bu_name

# ── Service Topology (enriched with business context) ────────────────────

SERVICES = {
    # ── Supply Chain ──
    "check-inventory-api": {
        "domain": "application", "tier": "critical",
        "depends_on": ["erp-sap-connector", "auth-service", "api-gateway"],
        "host_prefix": "inv-api-pod", "port": 8080,
    },
    "order-management-service": {
        "domain": "application", "tier": "critical",
        "depends_on": ["check-inventory-api", "shipment-routing-service", "auth-service"],
        "host_prefix": "oms-pod", "port": 8081,
    },
    "shipment-routing-service": {
        "domain": "application", "tier": "high",
        "depends_on": ["3pl-integration-api", "api-gateway"],
        "host_prefix": "ship-pod", "port": 8082,
    },
    "distribution-portal": {
        "domain": "application", "tier": "high",
        "depends_on": ["order-management-service", "auth-service"],
        "host_prefix": "dist-portal-pod", "port": 3000,
    },
    "3pl-integration-api": {
        "domain": "application", "tier": "high",
        "depends_on": ["api-gateway", "secrets-manager"],
        "host_prefix": "3pl-pod", "port": 8083,
    },
    "erp-sap-connector": {
        "domain": "infrastructure", "tier": "critical",
        "depends_on": ["dns-resolver"],
        "host_prefix": "erp-sap", "port": 8443,
    },
    # ── Digital Surgery ──
    "sagemaker-inference-endpoint": {
        "domain": "infrastructure", "tier": "critical",
        "depends_on": ["feature-store-api", "model-registry-service"],
        "host_prefix": "sm-ep", "port": 8501,
    },
    "ml-training-pipeline": {
        "domain": "application", "tier": "high",
        "depends_on": ["sagemaker-inference-endpoint", "feature-store-api", "data-mesh-platform"],
        "host_prefix": "mlpipe-pod", "port": 8090,
    },
    "ds-notebook-platform": {
        "domain": "application", "tier": "high",
        "depends_on": ["sagemaker-inference-endpoint", "feature-store-api", "auth-service"],
        "host_prefix": "ds-nb-pod", "port": 8888,
    },
    "feature-store-api": {
        "domain": "application", "tier": "high",
        "depends_on": ["data-mesh-platform", "auth-service"],
        "host_prefix": "fs-pod", "port": 8091,
    },
    "model-registry-service": {
        "domain": "application", "tier": "medium",
        "depends_on": ["auth-service", "secrets-manager"],
        "host_prefix": "mr-pod", "port": 8092,
    },
    "surgical-robotics-api": {
        "domain": "application", "tier": "critical",
        "depends_on": ["sagemaker-inference-endpoint", "auth-service"],
        "host_prefix": "surg-pod", "port": 8093,
    },
    # ── Clinical Trials ──
    "ctms-api": {
        "domain": "application", "tier": "critical",
        "depends_on": ["auth-service", "api-gateway", "data-mesh-platform"],
        "host_prefix": "ctms-pod", "port": 8100,
    },
    "edatacapture-service": {
        "domain": "application", "tier": "critical",
        "depends_on": ["ctms-api", "auth-service"],
        "host_prefix": "edc-pod", "port": 8101,
    },
    "randomization-service": {
        "domain": "application", "tier": "high",
        "depends_on": ["ctms-api", "secrets-manager"],
        "host_prefix": "rand-pod", "port": 8102,
    },
    "adverse-event-reporter": {
        "domain": "application", "tier": "critical",
        "depends_on": ["ctms-api", "regulatory-submission-api"],
        "host_prefix": "ae-pod", "port": 8103,
    },
    "regulatory-submission-api": {
        "domain": "application", "tier": "high",
        "depends_on": ["auth-service", "secrets-manager"],
        "host_prefix": "reg-pod", "port": 8104,
    },
    # ── Commercial Pharma ──
    "crm-integration-api": {
        "domain": "application", "tier": "high",
        "depends_on": ["api-gateway", "auth-service"],
        "host_prefix": "crm-pod", "port": 8110,
    },
    "hcp-portal": {
        "domain": "application", "tier": "high",
        "depends_on": ["crm-integration-api", "auth-service", "contract-pricing-api"],
        "host_prefix": "hcp-pod", "port": 3001,
    },
    "sample-management-service": {
        "domain": "application", "tier": "medium",
        "depends_on": ["crm-integration-api", "check-inventory-api"],
        "host_prefix": "samp-pod", "port": 8111,
    },
    "contract-pricing-api": {
        "domain": "application", "tier": "high",
        "depends_on": ["data-mesh-platform", "auth-service"],
        "host_prefix": "price-pod", "port": 8112,
    },
    "rebate-processing-service": {
        "domain": "application", "tier": "medium",
        "depends_on": ["contract-pricing-api", "erp-sap-connector"],
        "host_prefix": "rebate-pod", "port": 8113,
    },
    # ── Shared Infrastructure ──
    "auth-service": {
        "domain": "infrastructure", "tier": "critical",
        "depends_on": ["identity-provider", "secrets-manager"],
        "host_prefix": "auth-pod", "port": 8085,
    },
    "api-gateway": {
        "domain": "network", "tier": "critical",
        "depends_on": ["dns-resolver", "auth-service"],
        "host_prefix": "apigw", "port": 443,
    },
    "data-mesh-platform": {
        "domain": "infrastructure", "tier": "high",
        "depends_on": ["auth-service"],
        "host_prefix": "dmesh-pod", "port": 8200,
    },
    "identity-provider": {
        "domain": "infrastructure", "tier": "critical",
        "depends_on": [],
        "host_prefix": "idp", "port": 636,
    },
    "secrets-manager": {
        "domain": "infrastructure", "tier": "critical",
        "depends_on": [],
        "host_prefix": "sm", "port": 8201,
    },
    "dns-resolver": {
        "domain": "network", "tier": "critical",
        "depends_on": [],
        "host_prefix": "dns", "port": 53,
    },
}

ENVIRONMENTS = ["prod", "staging"]
REGIONS = ["us-east-1", "us-west-2"]

# ============================================================================
# FAILURE PATTERNS  -- The two anchor stories plus realistic supporting patterns
# ============================================================================

FAILURE_PATTERNS = [
    # ── STORY 1: Supply Chain Disruption ─────────────────────────────────
    {
        "id": "FP-SC-001",
        "name": "check-inventory-api Network Timeout to SAP ERP",
        "description": "Network timeout between check-inventory-api and erp-sap-connector causes cascading order fulfillment failures. Orders cannot be validated against available inventory.",
        "root_service": "check-inventory-api",
        "impacted_services": [
            "check-inventory-api", "order-management-service",
            "shipment-routing-service", "distribution-portal", "3pl-integration-api",
        ],
        "domain": "application",
        "secondary_domain": "network",
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (6, 10),
        "probability": 0.08,
        "avg_duration_min": 87,
        "business_impact": {
            "type": "shipment_throughput",
            "shipments_per_hour": 340,
            "avg_order_value_usd": 12500,
            "affected_users": 57,
            "affected_roles": ["logistics-coordinator", "warehouse-manager"],
            "servicenow_tickets_total": 8,
            "servicenow_tickets_duplicate": 3,
        },
        "root_cause_explanation": "VPC security group rule update blocked port 8443 traffic from check-inventory-api to erp-sap-connector. The API began returning HTTP 503 after connection pool exhaustion.",
    },
    # ── STORY 2: Digital Surgery Data Science Productivity Loss ──────────
    {
        "id": "FP-DS-001",
        "name": "SageMaker VPC Routing Misconfiguration -- Packet Loss",
        "description": "Routine VPC peering update introduced a route table conflict causing 40-60% packet loss on the us-east-1 SageMaker inference endpoint. ML workloads timed out or returned corrupt results.",
        "root_service": "sagemaker-inference-endpoint",
        "impacted_services": [
            "sagemaker-inference-endpoint", "ml-training-pipeline",
            "ds-notebook-platform", "feature-store-api", "model-registry-service",
        ],
        "domain": "infrastructure",
        "secondary_domain": "network",
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (8, 11),
        "probability": 0.05,
        "avg_duration_min": 183,
        "business_impact": {
            "type": "productivity_loss",
            "affected_data_scientists": 100,
            "productivity_loss_hours": 3,
            "loaded_rate_per_hour_usd": 1500,
            "total_productivity_loss_usd": 450000,
            "affected_users": 100,
            "affected_roles": ["data-scientist", "ml-engineer"],
            "servicenow_tickets_total": 15,
            "servicenow_tickets_duplicate": 12,
            "division": "digital-surgery",
        },
        "root_cause_explanation": "A Terraform change to VPC peering routes added a more-specific route for 10.20.0.0/16 that conflicted with existing SageMaker VPC endpoint routing. The more-specific route caused traffic to bypass the VPC endpoint and hit public internet with restrictive NACLs.",
    },
    # ── Supply Chain: ERP SAP connector batch timeout ────────────────────
    {
        "id": "FP-SC-002",
        "name": "ERP SAP Connector Batch Sync Overload",
        "description": "Daily SAP batch synchronization overwhelms erp-sap-connector during morning peak, causing cascading timeouts in supply chain services.",
        "root_service": "erp-sap-connector",
        "impacted_services": [
            "erp-sap-connector", "check-inventory-api",
            "order-management-service", "rebate-processing-service",
        ],
        "domain": "infrastructure",
        "secondary_domain": None,
        "severity": "P2",
        "trigger_day": None,
        "trigger_hour_range": (5, 7),
        "probability": 0.30,
        "avg_duration_min": 45,
        "business_impact": {
            "type": "shipment_throughput",
            "shipments_per_hour": 120,
            "avg_order_value_usd": 12500,
            "affected_users": 20,
            "affected_roles": ["logistics-coordinator"],
            "servicenow_tickets_total": 3,
            "servicenow_tickets_duplicate": 1,
        },
        "root_cause_explanation": "SAP IDOC processing queue saturated during nightly batch sync overlap with morning order intake. Connection pool exhausted on erp-sap-connector.",
    },
    # ── Clinical Trials: eDC data lock failure ───────────────────────────
    {
        "id": "FP-CT-001",
        "name": "eDataCapture Data Lock Cascade Failure",
        "description": "Database deadlock during concurrent data lock operations causes eDataCapture timeouts, blocking clinical trial data submission for multiple sites.",
        "root_service": "edatacapture-service",
        "impacted_services": [
            "edatacapture-service", "ctms-api",
            "adverse-event-reporter", "randomization-service",
        ],
        "domain": "application",
        "secondary_domain": None,
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (14, 17),
        "probability": 0.06,
        "avg_duration_min": 120,
        "business_impact": {
            "type": "trial_delay",
            "trial_delay_days": 0.5,
            "trial_delay_cost_per_day_usd": 350000,
            "affected_users": 35,
            "affected_roles": ["clinical-research-associate", "data-manager"],
            "servicenow_tickets_total": 6,
            "servicenow_tickets_duplicate": 2,
        },
        "root_cause_explanation": "Concurrent data lock operations from 12 trial sites created a deadlock cycle in the PostgreSQL backend. The connection pool exhausted while waiting for lock resolution.",
    },
    # ── Clinical Trials: Adverse event reporting slowdown ────────────────
    {
        "id": "FP-CT-002",
        "name": "Adverse Event Reporter API Gateway Throttling",
        "description": "API gateway rate-limits adverse-event-reporter during regulatory submission windows, delaying safety reporting.",
        "root_service": "adverse-event-reporter",
        "impacted_services": [
            "adverse-event-reporter", "regulatory-submission-api", "ctms-api",
        ],
        "domain": "application",
        "secondary_domain": "network",
        "severity": "P2",
        "trigger_day": None,
        "trigger_hour_range": (9, 12),
        "probability": 0.12,
        "avg_duration_min": 60,
        "business_impact": {
            "type": "trial_delay",
            "trial_delay_days": 0.25,
            "trial_delay_cost_per_day_usd": 350000,
            "affected_users": 15,
            "affected_roles": ["pharmacovigilance-specialist", "medical-monitor"],
            "servicenow_tickets_total": 4,
            "servicenow_tickets_duplicate": 1,
        },
        "root_cause_explanation": "API gateway throttle policy set to 100 req/s was too low for quarterly adverse event batch submissions. Burst traffic from multiple trial sites exceeded the limit.",
    },
    # ── Commercial Pharma: CRM integration outage ────────────────────────
    {
        "id": "FP-CP-001",
        "name": "CRM Integration OAuth Token Expiry",
        "description": "OAuth refresh token for Salesforce CRM integration expired, blocking HCP portal and sample management updates for commercial reps.",
        "root_service": "crm-integration-api",
        "impacted_services": [
            "crm-integration-api", "hcp-portal",
            "sample-management-service",
        ],
        "domain": "application",
        "secondary_domain": None,
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (0, 23),
        "probability": 0.04,
        "avg_duration_min": 95,
        "business_impact": {
            "type": "lost_sales",
            "revenue_per_hour_usd": 95000,
            "affected_users": 200,
            "affected_roles": ["pharma-sales-rep", "medical-science-liaison"],
            "servicenow_tickets_total": 22,
            "servicenow_tickets_duplicate": 16,
        },
        "root_cause_explanation": "Salesforce OAuth2 refresh token rotated by security policy without updating the secrets-manager entry. crm-integration-api returned 401 on all CRM sync calls.",
    },
    # ── Commercial Pharma: Contract pricing calculation error ────────────
    {
        "id": "FP-CP-002",
        "name": "Contract Pricing Engine Calculation Timeout",
        "description": "Complex rebate calculations for quarterly pricing refresh cause contract-pricing-api to timeout, blocking rebate processing and HCP portal pricing.",
        "root_service": "contract-pricing-api",
        "impacted_services": [
            "contract-pricing-api", "rebate-processing-service", "hcp-portal",
        ],
        "domain": "application",
        "secondary_domain": None,
        "severity": "P2",
        "trigger_day": None,
        "trigger_hour_range": (2, 5),
        "probability": 0.15,
        "avg_duration_min": 40,
        "business_impact": {
            "type": "lost_sales",
            "revenue_per_hour_usd": 95000,
            "affected_users": 50,
            "affected_roles": ["pricing-analyst", "contract-manager"],
            "servicenow_tickets_total": 5,
            "servicenow_tickets_duplicate": 2,
        },
        "root_cause_explanation": "Quarterly rebate recalculation with 50K+ contract lines exhausted the 30s query timeout on contract-pricing-api. The batch needed 120s+ to complete.",
    },
    # ── Shared Infrastructure: Auth service cascade ──────────────────────
    {
        "id": "FP-SI-001",
        "name": "Identity Provider LDAP Timeout Cascade",
        "description": "Identity provider intermittent LDAP timeouts cause auth-service thread pool exhaustion, blocking all authenticated services across business units.",
        "root_service": "auth-service",
        "impacted_services": [
            "auth-service", "api-gateway", "check-inventory-api",
            "ctms-api", "hcp-portal", "ds-notebook-platform",
        ],
        "domain": "infrastructure",
        "secondary_domain": None,
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (0, 23),
        "probability": 0.07,
        "avg_duration_min": 25,
        "business_impact": {
            "type": "blast_multiplier",
            "affected_users": 500,
            "affected_roles": [
                "logistics-coordinator", "data-scientist",
                "clinical-research-associate", "pharma-sales-rep",
            ],
            "servicenow_tickets_total": 35,
            "servicenow_tickets_duplicate": 28,
        },
        "root_cause_explanation": "LDAP connection pool on identity-provider hit max connections (200) during peak SSO traffic. Thread pool in auth-service blocked waiting for LDAP responses, causing auth failures across all BUs.",
    },
    # ── Shared Infrastructure: DNS resolver failures ─────────────────────
    {
        "id": "FP-SI-002",
        "name": "DNS Resolver Cache Poisoning",
        "description": "DNS resolver cache poisoning or TTL expiry causes intermittent name resolution failures across all services and business units.",
        "root_service": "dns-resolver",
        "impacted_services": [
            "dns-resolver", "api-gateway", "erp-sap-connector",
            "sagemaker-inference-endpoint", "crm-integration-api",
        ],
        "domain": "network",
        "secondary_domain": None,
        "severity": "P1",
        "trigger_day": None,
        "trigger_hour_range": (0, 23),
        "probability": 0.04,
        "avg_duration_min": 18,
        "business_impact": {
            "type": "blast_multiplier",
            "affected_users": 800,
            "affected_roles": [
                "logistics-coordinator", "data-scientist",
                "clinical-research-associate", "pharma-sales-rep",
            ],
            "servicenow_tickets_total": 40,
            "servicenow_tickets_duplicate": 32,
        },
        "root_cause_explanation": "Upstream DNS provider TTL set to 60s caused cache misses during a brief upstream outage. All internal DNS lookups failed for 3 minutes, triggering cascading connection failures.",
    },
    # ── Digital Surgery: ML training pipeline GPU OOM ────────────────────
    {
        "id": "FP-DS-002",
        "name": "ML Training Pipeline GPU Memory Leak",
        "description": "GPU memory leak in the surgical robotics model training pipeline causes OOM crashes after 48 hours, impacting model iteration velocity.",
        "root_service": "ml-training-pipeline",
        "impacted_services": [
            "ml-training-pipeline", "model-registry-service",
            "feature-store-api",
        ],
        "domain": "application",
        "secondary_domain": None,
        "severity": "P2",
        "trigger_day": None,
        "trigger_hour_range": (0, 23),
        "probability": 0.10,
        "avg_duration_min": 60,
        "business_impact": {
            "type": "productivity_loss",
            "affected_data_scientists": 25,
            "productivity_loss_hours": 1,
            "loaded_rate_per_hour_usd": 1500,
            "total_productivity_loss_usd": 37500,
            "affected_users": 25,
            "affected_roles": ["ml-engineer", "data-scientist"],
            "servicenow_tickets_total": 3,
            "servicenow_tickets_duplicate": 1,
            "division": "digital-surgery",
        },
        "root_cause_explanation": "PyTorch DataLoader workers accumulated GPU memory over 48-hour training runs. CUDA OOM triggered after ~40GB of 48GB A100 consumed. Required pod restart.",
    },
]

# ── Change Types ─────────────────────────────────────────────────────────
CHANGE_TYPES = [
    "deployment", "config_change", "scaling_event", "database_migration",
    "certificate_rotation", "firewall_rule_update", "dependency_upgrade",
    "feature_flag_toggle", "infra_patch", "network_route_change",
    "vpc_peering_update", "security_group_change", "terraform_apply",
]

# ── Utility ──────────────────────────────────────────────────────────────
random.seed(42)


def ts_to_unix_nano(dt):
    return int(dt.timestamp() * 1_000_000_000)


def make_resource_attrs(service_name):
    svc = SERVICES[service_name]
    instance_id = f"{svc['host_prefix']}-{random.randint(1, 5)}"
    bu = SERVICE_TO_BU.get(service_name, "shared-infrastructure")
    return {
        "service.name": service_name,
        "service.namespace": "dbrks-enterprise",
        "service.instance.id": instance_id,
        "host.name": f"{instance_id}.internal.dbrks.net",
        "deployment.environment": random.choice(ENVIRONMENTS),
        "cloud.region": random.choice(REGIONS),
        "cloud.provider": "aws",
        "service.version": f"{random.randint(1, 4)}.{random.randint(0, 12)}.{random.randint(0, 50)}",
        "business.unit": bu,
    }


def calculate_revenue_impact(failure_pattern, duration_min):
    """Deterministic revenue calculation based on business unit model."""
    bi = failure_pattern.get("business_impact", {})
    impact_type = bi.get("type", "")
    hours = duration_min / 60

    if impact_type == "shipment_throughput":
        delayed = bi.get("shipments_per_hour", 100) * hours
        revenue = delayed * bi.get("avg_order_value_usd", 12500)
        return round(revenue, 2), round(delayed), 0

    elif impact_type == "productivity_loss":
        scientists = bi.get("affected_data_scientists", bi.get("affected_users", 50))
        loss_hours = bi.get("productivity_loss_hours", hours)
        rate = bi.get("loaded_rate_per_hour_usd", 1500)
        loss = scientists * loss_hours * rate
        return round(loss, 2), 0, round(loss, 2)

    elif impact_type == "trial_delay":
        delay_days = bi.get("trial_delay_days", max(0.25, hours / 8))
        cost = delay_days * bi.get("trial_delay_cost_per_day_usd", 350000)
        return round(cost, 2), 0, 0

    elif impact_type == "lost_sales":
        revenue = hours * bi.get("revenue_per_hour_usd", 95000)
        return round(revenue, 2), 0, 0

    elif impact_type == "blast_multiplier":
        # Shared infra: estimate based on affected user count * avg hourly rate
        users = bi.get("affected_users", 100)
        loss = users * hours * 150  # $150/hr blended rate
        return round(loss, 2), 0, 0

    return 0, 0, 0


# ============================================================================
# PROTOBUF WIRE FORMAT ENCODER
# Encodes OTLP MetricsData matching otlp_metrics.proto without needing protoc
# ============================================================================

def _encode_varint(value):
    parts = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


def _encode_tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


def _encode_bytes_field(field_number, data):
    return _encode_tag(field_number, 2) + _encode_varint(len(data)) + data


def _encode_string_field(field_number, s):
    if not s:
        return b""
    return _encode_bytes_field(field_number, s.encode("utf-8"))


def _encode_double_field(field_number, value):
    return _encode_tag(field_number, 1) + struct.pack("<d", value)


def _encode_fixed64_field(field_number, value):
    return _encode_tag(field_number, 1) + struct.pack("<Q", value)


def _encode_sfixed64_field(field_number, value):
    return _encode_tag(field_number, 1) + struct.pack("<q", value)


def _encode_varint_field(field_number, value):
    if value == 0:
        return b""
    return _encode_tag(field_number, 0) + _encode_varint(value)


def _encode_bool_field(field_number, value):
    if not value:
        return b""
    return _encode_tag(field_number, 0) + _encode_varint(1)


def encode_any_value(val_dict):
    data = b""
    if "string_value" in val_dict:
        data += _encode_string_field(1, val_dict["string_value"])
    elif "bool_value" in val_dict:
        data += _encode_bool_field(2, val_dict["bool_value"])
    elif "int_value" in val_dict:
        data += _encode_varint_field(3, val_dict["int_value"])
    elif "double_value" in val_dict:
        data += _encode_double_field(4, val_dict["double_value"])
    return data


def encode_key_value(key, value_dict):
    data = _encode_string_field(1, key)
    any_val = encode_any_value(value_dict)
    data += _encode_bytes_field(2, any_val)
    return data


def encode_number_data_point(dp):
    data = b""
    for attr in dp.get("attributes", []):
        kv = encode_key_value(attr["key"], attr["value"])
        data += _encode_bytes_field(1, kv)
    data += _encode_fixed64_field(2, dp["time_unix_nano"])
    if "as_double" in dp:
        data += _encode_double_field(4, dp["as_double"])
    elif "as_int" in dp:
        data += _encode_sfixed64_field(6, dp["as_int"])
    return data


def encode_histogram_data_point(dp):
    data = b""
    for attr in dp.get("attributes", []):
        kv = encode_key_value(attr["key"], attr["value"])
        data += _encode_bytes_field(1, kv)
    data += _encode_fixed64_field(2, dp["time_unix_nano"])
    data += _encode_varint_field(4, dp["count"])
    data += _encode_double_field(5, dp["sum"])
    if dp.get("bucket_counts"):
        packed = b""
        for bc in dp["bucket_counts"]:
            packed += _encode_varint(bc)
        data += _encode_bytes_field(6, packed)
    if dp.get("explicit_bounds"):
        packed = b""
        for eb in dp["explicit_bounds"]:
            packed += struct.pack("<d", eb)
        data += _encode_bytes_field(7, packed)
    return data


def encode_metric(metric):
    data = b""
    data += _encode_string_field(1, metric["name"])
    data += _encode_string_field(2, metric.get("description", ""))
    data += _encode_string_field(3, metric.get("unit", ""))
    if "gauge" in metric:
        gauge_data = b""
        for dp in metric["gauge"]["data_points"]:
            gauge_data += _encode_bytes_field(1, encode_number_data_point(dp))
        data += _encode_bytes_field(5, gauge_data)
    elif "sum" in metric:
        sum_data = b""
        for dp in metric["sum"]["data_points"]:
            sum_data += _encode_bytes_field(1, encode_number_data_point(dp))
        sum_data += _encode_varint_field(2, metric["sum"].get("aggregation_temporality", 0))
        if metric["sum"].get("is_monotonic"):
            sum_data += _encode_bool_field(3, True)
        data += _encode_bytes_field(7, sum_data)
    elif "histogram" in metric:
        hist_data = b""
        for dp in metric["histogram"]["data_points"]:
            hist_data += _encode_bytes_field(1, encode_histogram_data_point(dp))
        hist_data += _encode_varint_field(2, metric["histogram"].get("aggregation_temporality", 0))
        data += _encode_bytes_field(9, hist_data)
    return data


def encode_instrumentation_scope(scope):
    data = _encode_string_field(1, scope["name"])
    data += _encode_string_field(2, scope.get("version", ""))
    return data


def encode_scope_metrics(sm):
    data = _encode_bytes_field(1, encode_instrumentation_scope(sm["scope"]))
    for m in sm["metrics"]:
        data += _encode_bytes_field(2, encode_metric(m))
    return data


def encode_resource(resource):
    data = b""
    for attr in resource["attributes"]:
        kv = encode_key_value(attr["key"], attr["value"])
        data += _encode_bytes_field(1, kv)
    return data


def encode_resource_metrics(rm):
    data = _encode_bytes_field(1, encode_resource(rm["resource"]))
    for sm in rm["scope_metrics"]:
        data += _encode_bytes_field(2, encode_scope_metrics(sm))
    return data


def encode_metrics_data(metrics_data):
    data = b""
    for rm in metrics_data["resource_metrics"]:
        data += _encode_bytes_field(1, encode_resource_metrics(rm))
    return data


# ── Metric Generator (Protobuf) ──────────────────────────────────────
def generate_metrics(start_date, end_date, w):
    """Generate OTLP metrics as protobuf .pb files, one per service batch."""
    print("Generating OTLP metrics as protobuf ...")

    proto_path = os.path.join(os.path.dirname(__file__), "otlp_metrics.proto")
    if os.path.exists(proto_path):
        with open(proto_path, "rb") as f:
            w.files.upload(f"{VOLUME_PATH}/metrics/otlp_metrics.proto", f, overwrite=True)
        print("  Uploaded otlp_metrics.proto schema")

    all_services = list(SERVICES.keys())
    svc_batches = [[svc] for svc in all_services[:15]]
    if len(all_services) > 15:
        remaining = all_services[15:]
        for i, svc in enumerate(remaining):
            svc_batches[i % 15].append(svc)

    total_metrics = 0
    for batch_idx, svc_batch in enumerate(svc_batches):
        resource_metrics_list = []

        for svc_name in svc_batch:
            svc_info = SERVICES[svc_name]
            resource_attrs = make_resource_attrs(svc_name)

            scope_metrics_for_svc = []
            current = start_date

            while current < end_date:
                for hour in range(0, 24, 1):
                    ts = current.replace(hour=hour, minute=0, second=0)
                    time_nano = ts_to_unix_nano(ts)

                    is_failing = False
                    for fp in FAILURE_PATTERNS:
                        if fp["root_service"] == svc_name or svc_name in fp["impacted_services"]:
                            if fp["trigger_day"] is None or ts.weekday() == fp["trigger_day"]:
                                h_lo, h_hi = fp["trigger_hour_range"]
                                if h_lo <= hour <= h_hi:
                                    if random.random() < fp["probability"]:
                                        is_failing = True

                    cpu_base = random.uniform(15, 45) if svc_info["tier"] == "critical" else random.uniform(10, 30)
                    cpu_val = min(99.5, cpu_base + (random.uniform(30, 55) if is_failing else random.uniform(-5, 15)))
                    mem_val = random.uniform(40, 92) if is_failing else random.uniform(30, 70)
                    hist_count = random.randint(500, 5000)
                    hist_sum = random.uniform(5000, 50000) * (3 if is_failing else 1)
                    bucket_counts = [random.randint(0, 500) for _ in range(8)]
                    active_req = random.randint(50, 500) if is_failing else random.randint(10, 100)
                    db_conn = random.randint(80, 100) if (is_failing and "database" in svc_name) else random.randint(10, 50)

                    metrics = [
                        {
                            "name": "system.cpu.utilization", "unit": "percent",
                            "gauge": {"data_points": [{"time_unix_nano": time_nano, "as_double": round(cpu_val, 2),
                                                       "attributes": [{"key": "cpu.state", "value": {"string_value": "user"}}]}]}
                        },
                        {
                            "name": "system.memory.utilization", "unit": "percent",
                            "gauge": {"data_points": [{"time_unix_nano": time_nano, "as_double": round(mem_val, 2), "attributes": []}]}
                        },
                        {
                            "name": "http.server.request.duration", "unit": "ms",
                            "histogram": {
                                "data_points": [{"time_unix_nano": time_nano, "count": hist_count, "sum": round(hist_sum, 2),
                                                 "bucket_counts": bucket_counts,
                                                 "explicit_bounds": [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
                                                 "attributes": [{"key": "http.method", "value": {"string_value": "GET"}}]}],
                                "aggregation_temporality": 2,
                            }
                        },
                        {
                            "name": "http.server.active_requests", "unit": "1",
                            "sum": {"data_points": [{"time_unix_nano": time_nano, "as_int": active_req, "attributes": []}],
                                    "aggregation_temporality": 2, "is_monotonic": False}
                        },
                        {
                            "name": "db.client.connections.usage", "unit": "connections",
                            "gauge": {"data_points": [{"time_unix_nano": time_nano, "as_int": db_conn,
                                                       "attributes": [{"key": "state", "value": {"string_value": "used"}}]}]}
                        },
                    ]

                    scope_metrics_for_svc.append({
                        "scope": {"name": "otel-collector", "version": "0.92.0"},
                        "metrics": metrics,
                    })
                    total_metrics += len(metrics)

                current += timedelta(days=1)

            resource_metrics_list.append({
                "resource": {
                    "attributes": [{"key": k, "value": {"string_value": v}} for k, v in resource_attrs.items()]
                },
                "scope_metrics": scope_metrics_for_svc,
            })

        metrics_data = {"resource_metrics": resource_metrics_list}
        pb_bytes = encode_metrics_data(metrics_data)

        primary_svc = svc_batch[0]
        file_name = f"metrics_{batch_idx:02d}_{primary_svc}.pb"
        file_path = f"{VOLUME_PATH}/metrics/{file_name}"
        buf = io.BytesIO(pb_bytes)
        w.files.upload(file_path, buf, overwrite=True)
        print(f"  Written {file_name} ({len(pb_bytes):,} bytes, services: {svc_batch})")

    print(f"  Metrics complete: {len(svc_batches)} .pb files, {total_metrics:,} total metric data points.")


# ── Log Generator (consolidated into 12 files) ───────────────────────

LOG_MESSAGES = {
    "normal": [
        "Request processed successfully",
        "Health check passed",
        "Cache hit for record",
        "Connection pool healthy",
        "Batch processing completed",
        "API call acknowledged",
        "Resource validated",
        "Authentication token refreshed",
    ],
    "warning": [
        "Connection pool utilization above 80%",
        "Request latency exceeding SLA threshold",
        "Retry attempt {n} for downstream call",
        "Memory utilization approaching limit",
        "Queue depth above normal threshold",
        "Certificate expiry in 7 days",
        "Slow query detected: {duration}ms",
        "Rate limit approaching for external API",
    ],
    "error": [
        "Connection pool exhausted - all connections in use",
        "Downstream service timeout after 30000ms",
        "API request parsing failed: invalid payload",
        "Validation error: missing required field",
        "Authentication service unreachable",
        "Data transfer failed: connection reset",
        "Out of memory: cannot allocate buffer",
        "TLS handshake failed: certificate expired",
        "Downstream service returned HTTP 503",
        "Consumer disconnected from message queue",
    ],
}

SEVERITIES = {
    "normal": ("INFO", 9),
    "warning": ("WARN", 13),
    "error": ("ERROR", 17),
    "fatal": ("FATAL", 21),
}


def generate_logs(start_date, end_date, w):
    """Generate structured log JSONL files -- 12 files (~15 days each)."""
    print("Generating structured logs (12 consolidated files) ...")
    total_days = (end_date - start_date).days
    days_per_file = total_days // 12

    current = start_date
    file_idx = 0
    total_logs = 0

    while current < end_date:
        chunk_end = min(current + timedelta(days=days_per_file), end_date)
        chunk_logs = []
        day = current

        while day < chunk_end:
            for hour in range(24):
                for minute in range(0, 60, 5):
                    ts = day.replace(hour=hour, minute=minute, second=random.randint(0, 59))
                    for svc_name in random.sample(list(SERVICES.keys()), k=min(10, len(SERVICES))):
                        resource_attrs = make_resource_attrs(svc_name)

                        is_failing = False
                        active_pattern = None
                        for fp in FAILURE_PATTERNS:
                            if fp["root_service"] == svc_name or svc_name in fp["impacted_services"]:
                                if fp["trigger_day"] is None or ts.weekday() == fp["trigger_day"]:
                                    h_lo, h_hi = fp["trigger_hour_range"]
                                    if h_lo <= hour <= h_hi and random.random() < fp["probability"]:
                                        is_failing = True
                                        active_pattern = fp

                        if is_failing:
                            severity_type = random.choice(["error", "error", "warning"])
                        else:
                            severity_type = random.choices(["normal", "normal", "normal", "warning"], weights=[70, 15, 10, 5])[0]

                        sev_text, sev_num = SEVERITIES[severity_type]
                        msg = random.choice(LOG_MESSAGES.get(severity_type, LOG_MESSAGES["normal"]))
                        msg = msg.replace("{n}", str(random.randint(1, 5))).replace("{duration}", str(random.randint(1000, 30000)))

                        log_record = {
                            "resourceLogs": [{
                                "resource": {"attributes": [{"key": k, "value": {"stringValue": v}} for k, v in resource_attrs.items()]},
                                "scopeLogs": [{
                                    "scope": {"name": svc_name},
                                    "logRecords": [{
                                        "timeUnixNano": str(ts_to_unix_nano(ts)),
                                        "observedTimeUnixNano": str(ts_to_unix_nano(ts) + random.randint(0, 1000000)),
                                        "severityNumber": sev_num,
                                        "severityText": sev_text,
                                        "body": {"stringValue": msg},
                                        "attributes": [
                                            {"key": "log.source", "value": {"stringValue": svc_name}},
                                            {"key": "thread.id", "value": {"intValue": str(random.randint(1, 200))}},
                                        ] + ([{"key": "failure_pattern", "value": {"stringValue": active_pattern["id"]}}] if active_pattern else []),
                                        "traceId": uuid.uuid4().hex,
                                        "spanId": uuid.uuid4().hex[:16],
                                    }]
                                }]
                            }]
                        }
                        chunk_logs.append(log_record)
            day += timedelta(days=1)

        file_path = f"{VOLUME_PATH}/logs/logs_chunk_{file_idx:02d}.jsonl"
        content = "\n".join(json.dumps(l) for l in chunk_logs)
        buf = io.BytesIO(content.encode("utf-8"))
        w.files.upload(file_path, buf, overwrite=True)
        total_logs += len(chunk_logs)
        file_idx += 1
        print(f"  Written logs_chunk_{file_idx-1:02d}.jsonl ({len(chunk_logs):,} records, {current.date()} to {chunk_end.date()})")
        current = chunk_end

    print(f"  Logs complete: {file_idx} files, {total_logs:,} total records.")


# ── Trace Generator (consolidated into 10 files) ─────────────────────

TRACE_OPERATIONS = {
    "check-inventory-api": ["GET /inventory/check", "POST /inventory/reserve", "GET /inventory/levels"],
    "order-management-service": ["POST /orders/create", "GET /orders/{id}", "PUT /orders/fulfill"],
    "shipment-routing-service": ["POST /shipment/route", "GET /shipment/status"],
    "distribution-portal": ["GET /portal/dashboard", "POST /portal/dispatch"],
    "sagemaker-inference-endpoint": ["POST /invocations", "GET /ping"],
    "ml-training-pipeline": ["POST /train/start", "GET /train/status"],
    "ds-notebook-platform": ["POST /notebook/execute", "GET /notebook/results"],
    "ctms-api": ["GET /trials/{id}", "POST /trials/enroll", "PUT /trials/update"],
    "edatacapture-service": ["POST /edc/submit", "GET /edc/forms", "PUT /edc/lock"],
    "crm-integration-api": ["POST /crm/sync", "GET /crm/contacts"],
    "hcp-portal": ["GET /hcp/dashboard", "POST /hcp/sample-request"],
    "auth-service": ["POST /auth/token", "POST /auth/validate", "POST /auth/refresh"],
    "api-gateway": ["ROUTE /api/*", "RATE_CHECK /api/*"],
    "contract-pricing-api": ["GET /pricing/calculate", "POST /pricing/rebate"],
}

# Group services into 10 clusters for trace files
TRACE_SERVICE_CLUSTERS = [
    ["check-inventory-api", "erp-sap-connector"],
    ["order-management-service", "shipment-routing-service"],
    ["sagemaker-inference-endpoint", "ml-training-pipeline"],
    ["ds-notebook-platform", "feature-store-api"],
    ["ctms-api", "edatacapture-service"],
    ["adverse-event-reporter", "regulatory-submission-api"],
    ["crm-integration-api", "hcp-portal"],
    ["contract-pricing-api", "rebate-processing-service"],
    ["auth-service", "identity-provider"],
    ["api-gateway", "dns-resolver"],
]


def generate_traces(start_date, end_date, w):
    """Generate distributed trace data -- 10 files, one per service cluster."""
    print("Generating distributed traces (10 consolidated files) ...")
    total_traces = 0

    for cluster_idx, cluster in enumerate(TRACE_SERVICE_CLUSTERS):
        cluster_traces = []
        current = start_date

        while current < end_date:
            for _ in range(50):
                hour = random.randint(6, 22)
                minute = random.randint(0, 59)
                ts = current.replace(hour=hour, minute=minute, second=random.randint(0, 59))
                trace_id = uuid.uuid4().hex

                entry_svc = cluster[0] if cluster[0] in TRACE_OPERATIONS else random.choice(list(TRACE_OPERATIONS.keys()))
                is_failing = False
                for fp in FAILURE_PATTERNS:
                    if entry_svc in [fp["root_service"]] + fp["impacted_services"]:
                        if fp["trigger_day"] is None or ts.weekday() == fp["trigger_day"]:
                            h_lo, h_hi = fp["trigger_hour_range"]
                            if h_lo <= hour <= h_hi and random.random() < fp["probability"]:
                                is_failing = True

                spans = []
                root_span_id = uuid.uuid4().hex[:16]
                root_duration = random.randint(50, 300) * (5 if is_failing else 1)
                ops = TRACE_OPERATIONS.get(entry_svc, ["GET /unknown"])

                spans.append({
                    "traceId": trace_id,
                    "spanId": root_span_id,
                    "parentSpanId": "",
                    "name": random.choice(ops),
                    "kind": 2,
                    "startTimeUnixNano": str(ts_to_unix_nano(ts)),
                    "endTimeUnixNano": str(ts_to_unix_nano(ts) + root_duration * 1_000_000),
                    "status": {"code": 2 if is_failing else 1, "message": "error" if is_failing else "ok"},
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": entry_svc}},
                        {"key": "http.status_code", "value": {"intValue": str(503 if is_failing else 200)}},
                    ],
                    "resource": {"attributes": [{"key": k, "value": {"stringValue": v}} for k, v in make_resource_attrs(entry_svc).items()]}
                })

                deps = SERVICES.get(entry_svc, {}).get("depends_on", [])
                for dep in deps:
                    if dep not in SERVICES:
                        continue
                    child_span_id = uuid.uuid4().hex[:16]
                    child_start = ts + timedelta(milliseconds=random.randint(5, 50))
                    child_duration = random.randint(10, 100) * (8 if is_failing else 1)
                    child_ops = TRACE_OPERATIONS.get(dep, [f"CALL {dep}"])

                    spans.append({
                        "traceId": trace_id,
                        "spanId": child_span_id,
                        "parentSpanId": root_span_id,
                        "name": random.choice(child_ops) if dep in TRACE_OPERATIONS else f"CALL {dep}",
                        "kind": 3,
                        "startTimeUnixNano": str(ts_to_unix_nano(child_start)),
                        "endTimeUnixNano": str(ts_to_unix_nano(child_start) + child_duration * 1_000_000),
                        "status": {"code": 2 if (is_failing and random.random() < 0.7) else 1},
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": dep}},
                            {"key": "peer.service", "value": {"stringValue": dep}},
                        ],
                        "resource": {"attributes": [{"key": k, "value": {"stringValue": v}} for k, v in make_resource_attrs(dep).items()]}
                    })

                cluster_traces.append({"resourceSpans": [{"resource": {}, "scopeSpans": [{"scope": {"name": "tracer"}, "spans": spans}]}]})
            current += timedelta(days=1)

        file_path = f"{VOLUME_PATH}/traces/traces_cluster_{cluster_idx:02d}_{cluster[0]}.json"
        content = json.dumps(cluster_traces, indent=None)
        buf = io.BytesIO(content.encode("utf-8"))
        w.files.upload(file_path, buf, overwrite=True)
        total_traces += len(cluster_traces)
        print(f"  Written traces_cluster_{cluster_idx:02d} ({len(cluster_traces):,} traces, services: {cluster})")

    print(f"  Traces complete: {len(TRACE_SERVICE_CLUSTERS)} files, {total_traces:,} total traces.")


# ── Event Generator (Incidents, Alerts, Changes) ─────────────────────

def generate_events(start_date, end_date, w):
    """Generate incident, alert, and change events with full business context."""
    print("Generating incident and alert events ...")
    all_incidents = []
    all_alerts = []
    all_changes = []
    current = start_date

    incident_counter = 1000
    alert_counter = 5000
    change_counter = 3000

    while current < end_date:
        for fp in FAILURE_PATTERNS:
            if fp.get("trigger_day") is not None and current.weekday() != fp["trigger_day"]:
                continue
            if random.random() > fp["probability"]:
                continue

            h_lo, h_hi = fp["trigger_hour_range"]
            start_hour = random.randint(h_lo, h_hi)
            incident_start = current.replace(hour=start_hour, minute=random.randint(0, 59))
            duration_min = fp["avg_duration_min"] + random.randint(-10, 20)
            duration_min = max(5, duration_min)
            incident_end = incident_start + timedelta(minutes=duration_min)

            incident_counter += 1
            blast_radius = len(fp["impacted_services"])

            # Deterministic revenue calculation
            revenue_impact, shipments_delayed, productivity_loss_usd = calculate_revenue_impact(fp, duration_min)

            bi = fp.get("business_impact", {})
            bu = SERVICE_TO_BU.get(fp["root_service"], "shared-infrastructure")
            affected_users = bi.get("affected_users", blast_radius * 10)
            affected_roles = bi.get("affected_roles", ["engineer"])
            sn_total = bi.get("servicenow_tickets_total", random.randint(1, 5))
            sn_dupes = bi.get("servicenow_tickets_duplicate", max(0, sn_total - random.randint(1, 3)))

            # Build human-readable narrative
            hours = duration_min / 60
            if bi.get("type") == "shipment_throughput":
                narrative = f"{round(bi.get('shipments_per_hour', 0) * hours)} shipments delayed over {hours:.1f} hrs x ${bi.get('avg_order_value_usd', 0):,} avg order value = ${revenue_impact:,.0f} revenue at risk"
            elif bi.get("type") == "productivity_loss":
                scientists = bi.get("affected_data_scientists", bi.get("affected_users", 0))
                loss_hrs = bi.get("productivity_loss_hours", hours)
                rate = bi.get("loaded_rate_per_hour_usd", 1500)
                narrative = f"{scientists} data scientists lost {loss_hrs} hrs productivity x ${rate:,}/hr loaded rate = ${productivity_loss_usd:,.0f} productivity loss"
            elif bi.get("type") == "trial_delay":
                delay_days = bi.get("trial_delay_days", max(0.25, hours / 8))
                narrative = f"{delay_days} day trial delay x ${bi.get('trial_delay_cost_per_day_usd', 350000):,}/day = ${revenue_impact:,.0f} trial delay cost"
            elif bi.get("type") == "lost_sales":
                narrative = f"{hours:.1f} hrs downtime x ${bi.get('revenue_per_hour_usd', 95000):,}/hr = ${revenue_impact:,.0f} lost sales revenue"
            else:
                narrative = f"{affected_users} users impacted across {blast_radius} services for {duration_min} min"

            incident = {
                "incident_id": f"INC-{incident_counter}",
                "title": fp["name"],
                "description": fp["description"],
                "severity": fp["severity"],
                "status": "resolved",
                "created_at": incident_start.isoformat(),
                "resolved_at": incident_end.isoformat(),
                "mttr_minutes": duration_min,
                "root_service": fp["root_service"],
                "impacted_services": fp["impacted_services"],
                "blast_radius": blast_radius,
                "domain": fp.get("domain", SERVICES.get(fp["root_service"], {}).get("domain", "unknown")),
                "failure_pattern_id": fp["id"],
                "failure_pattern_name": fp["name"],
                "environment": "prod",
                "region": random.choice(REGIONS),
                "revenue_impact_usd": revenue_impact,
                "sla_breached": fp["severity"] == "P1" and duration_min > 30,
                # ── New business context fields ──
                "business_unit": bu,
                "affected_user_count": affected_users,
                "affected_roles": affected_roles,
                "productivity_loss_hours": round(hours, 2),
                "productivity_loss_usd": productivity_loss_usd,
                "shipments_delayed": shipments_delayed,
                "servicenow_ticket_count": sn_total,
                "servicenow_duplicate_tickets": sn_dupes,
                "downstream_impact_narrative": narrative,
                "root_cause_explanation": fp.get("root_cause_explanation", ""),
                "revenue_model": bi.get("type", "unknown"),
            }
            all_incidents.append(incident)

            # Generate alerts for this incident
            for i in range(random.randint(2, 6)):
                alert_counter += 1
                alert_time = incident_start - timedelta(minutes=random.randint(1, 15))
                alert_svc = random.choice([fp["root_service"]] + fp["impacted_services"][:3])
                alert = {
                    "alert_id": f"ALT-{alert_counter}",
                    "incident_id": f"INC-{incident_counter}",
                    "service": alert_svc,
                    "alert_name": random.choice([
                        "HighCPUUtilization", "HighMemoryUsage", "HighLatency",
                        "ConnectionPoolNearCapacity", "ErrorRateSpike", "QueueDepthHigh",
                        "DiskIOSaturation", "ThreadPoolExhaustion",
                    ]),
                    "severity": random.choice(["critical", "warning", "warning"]),
                    "fired_at": alert_time.isoformat(),
                    "resolved_at": (alert_time + timedelta(minutes=random.randint(10, 60))).isoformat(),
                    "threshold_value": round(random.uniform(80, 99), 1),
                    "actual_value": round(random.uniform(85, 100), 1),
                    "domain": SERVICES.get(alert_svc, {}).get("domain", "unknown"),
                    "environment": "prod",
                }
                all_alerts.append(alert)

        # Daily changes
        num_changes = random.randint(0, 3)
        for _ in range(num_changes):
            change_counter += 1
            change_svc = random.choice(list(SERVICES.keys()))
            change_time = current.replace(hour=random.randint(6, 22), minute=random.randint(0, 59))
            change = {
                "change_id": f"CHG-{change_counter}",
                "service": change_svc,
                "change_type": random.choice(CHANGE_TYPES),
                "description": f"{random.choice(CHANGE_TYPES).replace('_', ' ').title()} for {change_svc}",
                "executed_at": change_time.isoformat(),
                "executed_by": random.choice(["deploy-bot", "sre-team", "dev-team", "infra-automation", "security-scanner", "terraform-ci"]),
                "risk_level": random.choice(["low", "medium", "high"]),
                "rollback_available": random.choice([True, True, True, False]),
                "domain": SERVICES[change_svc]["domain"],
                "environment": random.choice(ENVIRONMENTS),
                "region": random.choice(REGIONS),
            }
            all_changes.append(change)

        # Background noise alerts (not incident-correlated)
        for _ in range(random.randint(3, 10)):
            alert_counter += 1
            alert_svc = random.choice(list(SERVICES.keys()))
            alert_time = current.replace(hour=random.randint(0, 23), minute=random.randint(0, 59))
            alert = {
                "alert_id": f"ALT-{alert_counter}",
                "incident_id": None,
                "service": alert_svc,
                "alert_name": random.choice([
                    "HighCPUUtilization", "HighMemoryUsage", "HighLatency",
                    "ConnectionPoolNearCapacity", "ErrorRateSpike", "QueueDepthHigh",
                    "GarbageCollectionPause", "SlowQuery", "CertificateExpiringSoon",
                ]),
                "severity": random.choice(["info", "warning", "warning", "critical"]),
                "fired_at": alert_time.isoformat(),
                "resolved_at": (alert_time + timedelta(minutes=random.randint(5, 120))).isoformat(),
                "threshold_value": round(random.uniform(70, 95), 1),
                "actual_value": round(random.uniform(75, 100), 1),
                "domain": SERVICES[alert_svc]["domain"],
                "environment": random.choice(ENVIRONMENTS),
            }
            all_alerts.append(alert)

        current += timedelta(days=1)

    print(f"  Generated {len(all_incidents)} incidents, {len(all_alerts)} alerts, {len(all_changes)} changes.")

    content = "\n".join(json.dumps(inc) for inc in all_incidents)
    buf = io.BytesIO(content.encode("utf-8"))
    w.files.upload(f"{VOLUME_PATH}/events/incidents.jsonl", buf, overwrite=True)

    content = "\n".join(json.dumps(a) for a in all_alerts)
    buf = io.BytesIO(content.encode("utf-8"))
    w.files.upload(f"{VOLUME_PATH}/events/alerts.jsonl", buf, overwrite=True)

    content = "\n".join(json.dumps(c) for c in all_changes)
    buf = io.BytesIO(content.encode("utf-8"))
    w.files.upload(f"{VOLUME_PATH}/events/topology_changes.jsonl", buf, overwrite=True)

    print("  Events complete.")
    return len(all_incidents), len(all_alerts), len(all_changes)


def main():
    w = WorkspaceClient(profile=PROFILE)
    if raw_volume_has_data(w):
        print(f"Volume {VOLUME_PATH} already contains data. Skipping generation to avoid overwriting.")
        print("Delete or clear the volume subdirs (metrics, logs, traces, events) if you want to regenerate.")
        return

    end_date = datetime(2026, 2, 25, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=30)

    print(f"Generating telemetry from {start_date.date()} to {end_date.date()} (30 days)")
    print(f"Volume: {VOLUME_PATH}")
    print()

    generate_metrics(start_date, end_date, w)
    generate_logs(start_date, end_date, w)
    generate_traces(start_date, end_date, w)
    generate_events(start_date, end_date, w)

    print("\nAll raw telemetry data generated successfully.")


if __name__ == "__main__":
    main()
