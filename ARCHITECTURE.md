# Architecture

## System Overview

Enterprise Root Cause Intelligence is a full-stack **Databricks App** that ingests OpenTelemetry signals (metrics, logs, traces, events, network flows) across infrastructure, application, and network domains, builds a Bronze/Silver/Gold medallion data model, and exposes an interactive React dashboard with AI-powered root cause analysis. Built for a Dbrks Enterprise Observability demo targeting life sciences (HLS) environments.

The system has three major parts: a **setup pipeline** that creates a Unity Catalog schema, generates synthetic data, and builds the medallion tables; a **data pipeline** (Databricks Job) that runs the same Bronze → Silver → Gold transforms on a schedule; and a **web application** (FastAPI + React) that queries the enriched tables and renders executive dashboards with AI-powered analysis.

> **Important assumption**: This demo starts with data already landed in a Unity Catalog Volume (object storage). In a production deployment, telemetry data would flow from operational observability tools into S3/ADLS/GCS via the ingestion patterns described below, then be picked up by the Bronze ingestion layer.

---

## Data Ingestion Patterns (Operational → Analytical)

The analytical plane in this demo consumes five signal types: **metrics**, **logs**, **traces**, **events** (incidents/alerts/changes), and **network flows**. In production, these signals originate from operational observability tools (Prometheus/VictoriaMetrics, Grafana Loki, Splunk, Kafka, ClickHouse, etc.) and must be landed in object storage before the medallion pipeline can process them.

The patterns below show the recommended architectures for bridging the **Operational Plane** (real-time alerting and dashboards) to the **Analytical Plane** (Databricks — correlation, root cause analysis, business impact). All patterns target **< 5 minute SLA** and prioritize durable, replayable file landing over direct API coupling.

### VictoriaMetrics → Metrics

![VictoriaMetrics Patterns](arch/page-08.png)

**Signal**: Time-series metrics (CPU, memory, latency, error rates, custom business metrics)

VictoriaMetrics exposes two extraction paths. The **recommended** approach uses the `/api/v1/export` endpoint to extract raw series as CSV or JSONL files landed directly into S3. This avoids the aggregation and time-alignment overhead of PromQL `query_range` and produces deterministic, replayable exports. Alternatively, TSDB blocks (written by VictoriaMetrics or shipped via a Thanos-style exporter) can be landed in S3 and translated to Delta.

**Key benefit**: Operational isolation — the analytical pipeline reads from S3, not from VictoriaMetrics directly, so analytical workloads never affect operational query performance.

### Grafana Loki → Logs

![Grafana Loki Patterns](arch/page-09.png)

**Signal**: Structured and unstructured logs (application logs, infrastructure events, audit trails)

Several ingestion paths exist for Loki-managed logs. The **recommended** approach is LogQL Export — scheduled queries that extract enriched log batches to S3 as JSON, since Promtail-based enrichment (labels, parsing) is preserved. The **best alternative** is OTel Collector dual-write: the collector sends logs to both Loki (for real-time operational use) and S3/JSON (for analytical ingestion), keeping Loki as the operational system of record while producing analytics-ready files. A Databricks ZeroBus OTLP endpoint is also available as a direct-ingest alternative.

**Key benefit**: Loki's chunk-based storage requires translation before analytics use; landing enriched exports in S3 decouples retention and query patterns between operational and analytical workloads.

### Kafka → Events & Streaming Telemetry

![Kafka Patterns](arch/page-10.png)

**Signal**: Incident events, alert events, topology changes, real-time telemetry streams

Kafka acts as the backbone for event-driven observability data. The **recommended** pattern uses the Kafka S3 Sink Connector to land Parquet files in S3, which Databricks picks up via Auto Loader for incremental ingestion. For lower-latency requirements, Databricks Structured Streaming can consume directly from Kafka topics, though this creates a runtime dependency on Kafka availability. ZeroBus offers a Kafka-alternative ingestion path where producers send directly to Databricks via REST/gRPC.

**Key benefit**: The S3 sink provides durability, replay safety, and rebuild capability — if the analytical pipeline fails, data is not lost and can be reprocessed from the landing zone.

### ClickHouse → Pre-aggregated Analytics

![ClickHouse Patterns](arch/page-11.png)

**Signal**: Pre-aggregated OLAP queries, materialized views, historical rollups

ClickHouse is used for columnar OLAP workloads in some observability stacks. The **recommended** pattern exports Parquet files to S3, then ingests via Databricks Auto Loader. An alternative JDBC pull pattern is available for simpler setups but creates runtime coupling. The Parquet export path scales better for large historical datasets and preserves ClickHouse's columnar efficiency through the transfer.

**Key benefit**: Object storage landing enables replay and decoupling — ClickHouse can be upgraded, migrated, or temporarily unavailable without affecting the analytical pipeline.

### Splunk → Logs, Metrics & Events

![Splunk Patterns](arch/page-12.png)

**Signal**: Splunk Enterprise logs (via SPL), Splunk Observability Cloud metrics/traces/events (via REST APIs)

Splunk environments offer multiple extraction paths. For **Splunk Enterprise**, scheduled SPL searches export time-bounded result files (CSV/JSON) to S3 — this is the recommended bulk extraction method. For **Splunk Observability Cloud**, REST APIs (`/v2/metric`, `/v2/dimension`, `/v2/timeserieswindow`, `/v2/detector`, `/v2/event`) provide targeted retrieval but are optimized for operational queries, not bulk export. The **recommended** approach is OTel Collector dual-write to S3 for durable analytics ingestion; use SPL export for Splunk Enterprise bulk extraction and REST APIs only for targeted retrieval.

**Key benefit**: Object storage landing provides replay capability, backfill safety, and analytical isolation without stressing the Splunk query plane.

### How This Maps to the Demo

In this demo, all five signal types are represented by synthetic data generated by the setup pipeline and landed in the Unity Catalog Volume (`raw_landing/`). The mapping to production sources would be:

| Signal Type | Demo Data | Production Source(s) |
|-------------|-----------|---------------------|
| Metrics | `raw_landing/metrics/*.pb` (OTLP protobuf) | VictoriaMetrics `/api/v1/export` → S3, or OTel Collector → S3 |
| Logs | `raw_landing/logs/*.jsonl` | Grafana Loki LogQL export → S3, or OTel Collector dual-write → S3 |
| Traces | `raw_landing/traces/*.json` | OTel Collector → S3 (JSON/Parquet), or Splunk Observability Cloud API |
| Events | `raw_landing/events/*.jsonl` (incidents, alerts, changes) | Kafka S3 sink → S3 (Parquet), ServiceNow/PagerDuty webhooks → Kafka → S3 |
| Network flows | `raw_landing/network_flows/*.pb` (custom binary) | Network flow collectors → Kafka → S3, or ClickHouse export → S3 |

The Bronze ingestion layer is format-aware — it includes custom protobuf decoders for metrics and network flows, Spark SQL for JSONL/JSON, and can be extended to consume Parquet from Auto Loader when connected to real operational pipelines.

---

## Request Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Browser (React SPA on :5173 dev / served by FastAPI in prod)   │
└────────────────────────────────┬────────────────────────────────┘
                                 │ fetch("/api/...")
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  FastAPI  (rca_app/backend/main.py :8000)                       │
│                                                                 │
│  ├── /api/incidents/*        → incidents.py                     │
│  ├── /api/root-cause/*       → root_cause.py                    │
│  ├── /api/services/*         → service_ranking.py               │
│  ├── /api/changes/*          → change_correlation.py            │
│  ├── /api/domains/*          → domain_summary.py                │
│  ├── /api/genie/*            → genie.py                         │
│  ├── /api/health             → inline health check              │
│  └── /*                      → SPA catch-all (frontend/dist/)   │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
           │ SQL via                      │ HTTP (aiohttp)
           │ databricks-sdk               │
           ▼                              ▼
┌─────────────────────┐    ┌──────────────────────────────────────┐
│  Databricks SQL      │    │  Databricks APIs                     │
│  Warehouse           │    │  ├── Foundation Model API             │
│                      │    │  │   (databricks-claude-sonnet-4)     │
│  Gold/Silver/Bronze  │    │  └── Genie Space API                  │
│  Delta tables        │    │      (/api/2.0/genie/spaces/...)     │
└─────────────────────┘    └──────────────────────────────────────┘
```

---

## Component Details

### Backend (FastAPI)

**Framework**: **FastAPI** with **Uvicorn**, async via **aiohttp** for external API calls.

**Entry point**: `rca_app/app.py` — loads `.env` via **python-dotenv** (override=False so platform env vars win), then imports `backend.main:app` and starts Uvicorn.

| File | Purpose |
|------|---------|
| `backend/main.py` | App setup, CORS (localhost:5173/3000), router registration, SPA catch-all serving `frontend/dist/` |
| `backend/db.py` | Databricks SQL connection — `execute_query()` runs SQL via `statement_execution`, polls up to 60s. Warehouse discovery prefers serverless → running → first available. Auth: profile-based locally, service principal when deployed (`DATABRICKS_APP_NAME` present). |
| `backend/routes/incidents.py` | 8 endpoints: summary stats, timeline, recent, by-service, by-hour, MTTR trend, ticket noise, single incident detail |
| `backend/routes/root_cause.py` | 5 endpoints: patterns list, top systemic issue, pattern timeline, correlated signals, AI analysis (LLM with rule-based fallback) |
| `backend/routes/service_ranking.py` | 5 endpoints: risk ranking, health timeline, topology graph (nodes + edges from network flows + incidents), service incidents, service alerts |
| `backend/routes/change_correlation.py` | 5 endpoints: correlation summary, timeline, high-correlation pairs, risky change types, by-executor |
| `backend/routes/domain_summary.py` | 6 endpoints: summary, heatmap, trend, domain services (hardcoded domain→service mapping), domain incidents, domain alerts |
| `backend/routes/genie.py` | 2 endpoints: space-id, query (proxies Genie Space API with keyword-based SQL fallback for supply chain, digital surgery, tickets, revenue, blast radius) |

**Backend environment variables**:

| Field | Default | Purpose |
|-------|---------|---------|
| `CATALOG` | `bldemos` | Unity Catalog catalog name |
| `SCHEMA` | `eo_analytics` | Unity Catalog schema name |
| `DATABRICKS_PROFILE` | `DEFAULT` | CLI profile for local dev auth |
| `DATABRICKS_WAREHOUSE_ID` | (auto-discover) | SQL warehouse ID; set via app resource binding in prod |
| `DATABRICKS_APP_NAME` | (unset) | Presence triggers service principal auth path |
| `SERVING_ENDPOINT` | `databricks-claude-sonnet-4` | Foundation Model API model name for AI analysis |
| `GENIE_SPACE_ID` | (empty) | Genie Space ID; empty triggers SQL fallback mode |
| `PORT` | `8000` | Uvicorn listen port |

### Frontend (React)

**Framework**: **React 18** with **Vite** (JSX, no TypeScript). **Recharts** for charts, **Canvas API** for topology, **lucide-react** for icons, **react-router-dom** for routing, **react-markdown** for AI analysis rendering.

| File | Purpose |
|------|---------|
| `frontend/src/main.jsx` | React 18 `createRoot` entry with `BrowserRouter` |
| `frontend/src/App.jsx` | Sidebar nav layout with 4 sections (Overview, Analysis, Explore, Investigate) + route definitions |
| `frontend/src/hooks/useApi.js` | `useApi(endpoint)` GET hook, `useApiPost()` POST hook, plus `formatNumber`/`formatCurrency`/`formatDate` helpers |
| `frontend/src/index.css` | Dark theme CSS custom properties (Databricks-inspired), severity/domain color tokens, chart palette |

**Pages** (7 routes):

| Route | Page Component | Key Visualizations |
|-------|---------------|-------------------|
| `/` | ExecutiveDashboard | KPI stat cards, incident timeline (AreaChart), domain pie chart, ticket noise table |
| `/root-cause` | RootCauseIntelligence | Priority bar chart, 6-axis radar chart (frequency/MTTR/blast/revenue/user impact/SLA), AI analysis panel |
| `/service-risk` | ServiceRiskRanking | Risk score bars, incidents-vs-revenue scatter plot, health score line chart |
| `/change-correlation` | ChangeCorrelation | Changes+incidents timeline overlay (ComposedChart), risky change types bar chart, correlation table |
| `/domain-deep-dive` | DomainDeepDive | Domain selector tiles, weekly trend area chart, service risk bars, incident detail panel |
| `/topology` | TopologyExplorer | Canvas-rendered dependency graph with 3-layer layout (network/application/infrastructure), risk-encoded nodes/edges |
| `/genie` | GenieChat | ChatGPT-style chat with starter question chips, markdown responses, SQL display, data tables |

**Shared components**: `ChartTooltip` (Recharts custom tooltip), `LoadingState`/`ErrorState`/`EmptyState` (loading states), `SeverityBadge`/`DomainBadge`/`TrendBadge` (colored badges), `InfoExpander` (collapsible methodology notes).

### Data Pipeline (Databricks Job)

**Framework**: **PySpark** notebooks executed as a Databricks Job (defined in `databricks.yml`). All serverless compute — no cluster startup delay.

| Task | Notebook | Description |
|------|----------|-------------|
| `ingest_metrics` | `01_ingest_metrics_pb.py` | Custom protobuf wire-format decoder → `bronze_metrics` |
| `ingest_logs` | `02_ingest_logs.py` | JSONL → `bronze_logs` (Spark SQL) |
| `ingest_traces` | `03_ingest_traces.py` | JSON → `bronze_traces` (Spark SQL) |
| `ingest_events` | `04_ingest_events.py` | JSONL → `bronze_incidents`, `bronze_alerts`, `bronze_topology_changes` |
| `ingest_network_flows` | `05_ingest_network_flows_pb.py` | Custom NFLOW binary → `bronze_network_flows` |
| `build_silver` | `06_silver_transforms.py` | Bronze → 6 silver tables (enrichment, scoring, correlation) |
| `build_gold` | `07_gold_transforms.py` | Silver → 5 gold tables (patterns, ranking, correlation, domain, business) |

Bronze tasks run in parallel; `build_silver` waits for all bronze; `build_gold` waits for silver.

### Setup Pipeline (Local Scripts)

**Framework**: Pure Python using **databricks-sdk** for SQL execution and file uploads. Custom protobuf serializers (no `protobuf` library dependency).

| Script | Phase | Purpose |
|--------|-------|---------|
| `00_create_schema_and_volume.py` | Schema | Create UC schema + managed volume with subdirs |
| `01_generate_raw_telemetry.py` | Data gen | Generate OTLP metrics/logs/traces/events (skips if volume non-empty) |
| `02_generate_protobuf_network_flows.py` | Data gen | Generate network flow `.pb` files (skips if volume non-empty) |
| `03_create_bronze_tables.py` | Pipeline | Volume → bronze Delta tables |
| `04_create_silver_tables.py` | Pipeline | Bronze → silver (enrichment, scoring, correlation) |
| `05_create_gold_tables.py` | Pipeline | Silver → gold (analytics aggregations) |
| `06_create_genie_space.py` | Config | Create Genie Space (add tables via UI after) |
| `07_grant_app_uc_permissions.py` | Config | Grant app service principal SELECT on all tables |

Proto schemas (`otlp_metrics.proto`, `network_flow.proto`) live alongside the data generators in `setup_pipeline/` and document the binary formats.

---

## Infrastructure / External Services

```
Databricks Workspace (fevm-stable-classic-zso77x-bx3)
│
├── Unity Catalog
│   └── bldemos.eo_analytics
│       ├── Volume: raw_landing/
│       │   ├── metrics/        (OTLP .pb files)
│       │   ├── logs/           (JSONL files)
│       │   ├── traces/         (JSON files)
│       │   ├── events/         (JSONL — incidents, alerts, topology changes)
│       │   └── network_flows/  (.pb files, custom NFLOW binary format)
│       │
│       ├── Bronze tables (7)
│       │   bronze_metrics, bronze_logs, bronze_traces,
│       │   bronze_incidents, bronze_alerts,
│       │   bronze_topology_changes, bronze_network_flows
│       │
│       ├── Silver tables (6)
│       │   silver_incidents, silver_alerts, silver_changes,
│       │   silver_service_health, silver_business_impact,
│       │   silver_servicenow_correlation
│       │
│       └── Gold tables (5)
│           gold_root_cause_patterns, gold_service_risk_ranking,
│           gold_change_incident_correlation,
│           gold_domain_impact_summary, gold_business_impact_summary
│
├── SQL Warehouse: d119a93099e7209f (serverless)
│
├── Foundation Model API
│   └── Serving endpoint: databricks-claude-sonnet-4
│       Used for AI root cause analysis (POST /serving-endpoints/.../invocations)
│
├── Genie Space: 01f11276e8831838981f4c5743c5a3e3
│   Natural language Q&A over gold/silver tables
│   API: /api/2.0/genie/spaces/{id}/start-conversation
│
└── Databricks Apps
    └── dbrks-eo-analytics-demo (service principal auth)
```

---

## Data Pipeline

```
Volume (raw_landing/)
│
├── metrics/*.pb ─────────┐
├── logs/*.jsonl ──────────┤
├── traces/*.json ─────────┤── Bronze Ingestion (parallel)
├── events/*.jsonl ────────┤   Parse raw formats into structured Delta tables
├── network_flows/*.pb ───┘
│
▼
Bronze (7 tables)
│   Raw parsed records with original schema
│
▼ ── Silver Transform ──────────────────────────────────────────
│   silver_incidents     Enriched with severity_level, correlated alerts/changes, impact_score
│   silver_alerts        + duration, breach magnitude, pre-incident signal flag
│   silver_changes       + risk_score (risk_level × rollback × change_type), incident correlation windows
│   silver_service_health  Daily composite: 100 - cpu×0.15 - mem×0.10 - incidents×15 - p1×25 - errors×0.1
│   silver_business_impact  Revenue impact classification (critical/high/moderate/low)
│   silver_servicenow_correlation  Ticket dedup analysis (duplicate_pct)
│
▼ ── Gold Transform ────────────────────────────────────────────
│   gold_root_cause_patterns          Recurring failure signatures with priority_score + trend (worsening/improving/stable)
│   gold_service_risk_ranking         Composite risk: incidents×10 + p1×30 + sla×20 + revenue/10K + blast×5 + ...
│   gold_change_incident_correlation  Causal analysis: correlation_strength × window × service_match × risk
│   gold_domain_impact_summary        Daily domain-level aggregation with domain_risk_score
│   gold_business_impact_summary      Per-business-unit rollup: revenue, productivity, shipments, ServiceNow
```

---

## Deployment

### Local Development

```bash
cd rca_app

# Backend (terminal 1)
pip install -r requirements.txt
python app.py                                  # FastAPI on :8000 (reads .env automatically)

# Frontend (terminal 2)
cd frontend && npm install
npm run dev                                    # Vite on :5173, proxies /api → :8000
```

### Production Build (local)

```bash
cd rca_app
cd frontend && npm run build && cd ..          # Build SPA into frontend/dist/
python app.py                                  # FastAPI serves both API and SPA
```

### Databricks Apps Deployment

Driven by `databricks.yml` (DABs bundle) at the repo root.

```bash
# Deploy app + pipeline job
databricks bundle deploy --profile DEFAULT

# Run the data pipeline
databricks bundle run dbrks-eo-analytics-demo-pipeline --profile DEFAULT

# Or deploy with .env variable injection
./scripts/deploy_with_env.sh DEFAULT
```

**DABs configuration** (`databricks.yml`):

| Field | Value |
|-------|-------|
| Target | `default` (production mode, no resource name prefix) |
| Root path | `/Workspace/Users/robert.leach@databricks.com/dbrks-eo-analytics-demo` |
| App source | `./rca_app` |
| App command | `python app.py` |
| Job schedule | `0 0 2 * * ?` UTC (PAUSED) |
| Compute | Serverless (all tasks) |

### Data Setup (one-time)

```bash
# 1. Create schema + volume, generate synthetic data
python setup_pipeline/00_create_schema_and_volume.py
python setup_pipeline/01_generate_raw_telemetry.py
python setup_pipeline/02_generate_protobuf_network_flows.py

# 2. Build medallion tables
python setup_pipeline/03_create_bronze_tables.py
python setup_pipeline/04_create_silver_tables.py
python setup_pipeline/05_create_gold_tables.py

# 3. Configure Genie and permissions
python setup_pipeline/06_create_genie_space.py
python setup_pipeline/07_grant_app_uc_permissions.py
```

---

## Directory Structure

```
dbrks-eo-analytics-demo/
├── databricks.yml                  # DABs bundle: app + pipeline job definition
├── README.md                       # Quick start and env var documentation
├── CLAUDE.md                       # Claude Code guidance
├── ARCHITECTURE.md                 # This file
│
├── rca_app/                        # Databricks App (deployed as a unit)
│   ├── app.py                      # Uvicorn entry point — loads .env, starts backend.main:app
│   ├── app.yaml                    # Databricks App runtime config (command, env, warehouse binding)
│   ├── requirements.txt            # Python deps: fastapi, uvicorn, aiohttp, databricks-sdk, python-dotenv
│   ├── .env.example                # Template for local env vars
│   ├── .env                        # Local env values (gitignored)
│   │
│   ├── backend/
│   │   ├── __init__.py
│   │   ├── main.py                 # FastAPI app: CORS, router registration, SPA catch-all
│   │   ├── db.py                   # Databricks SQL: execute_query(), warehouse discovery, auth detection
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── incidents.py        # /api/incidents/* — summary, timeline, recent, by-service, MTTR, tickets
│   │       ├── root_cause.py       # /api/root-cause/* — patterns, top issue, AI analysis (LLM + fallback)
│   │       ├── service_ranking.py  # /api/services/* — risk ranking, health, topology graph
│   │       ├── change_correlation.py # /api/changes/* — correlation summary, timeline, risky types
│   │       ├── domain_summary.py   # /api/domains/* — summary, heatmap, trend, services/incidents/alerts
│   │       └── genie.py            # /api/genie/* — Genie Space proxy with SQL fallback
│   │
│   └── frontend/
│       ├── index.html              # Vite HTML entry
│       ├── package.json            # React 18, Recharts, lucide-react, react-router-dom, react-markdown
│       ├── vite.config.js          # Dev proxy: /api → localhost:8000
│       ├── dist/                   # Built SPA (checked into git for deployment)
│       └── src/
│           ├── main.jsx            # React 18 createRoot + BrowserRouter
│           ├── index.css           # Dark theme, CSS custom properties, severity/domain colors
│           ├── App.jsx             # Sidebar nav layout + route definitions
│           ├── hooks/
│           │   └── useApi.js       # useApi() GET hook, useApiPost(), format helpers
│           ├── components/
│           │   ├── ChartTooltip.jsx    # Shared Recharts tooltip
│           │   ├── LoadingState.jsx    # Loading/Error/Empty state components
│           │   ├── SeverityBadge.jsx   # Severity/Domain/Trend badge components
│           │   └── InfoExpander.jsx    # Collapsible methodology explainer
│           └── pages/
│               ├── ExecutiveDashboard.jsx     # KPI cards, incident timeline, domain pie, ticket noise
│               ├── RootCauseIntelligence.jsx  # Pattern ranking, radar chart, AI analysis panel
│               ├── ServiceRiskRanking.jsx     # Risk bars, scatter plot, health timeline
│               ├── ChangeCorrelation.jsx      # Changes vs incidents timeline, correlation table
│               ├── DomainDeepDive.jsx         # Domain selector, trend charts, service/alert tables
│               ├── TopologyExplorer.jsx       # Canvas dependency graph, 3-layer layout, drill mode
│               └── GenieChat.jsx              # Chat UI with starter questions, SQL display, data tables
│
├── data_pipelines/                 # Databricks Job notebook tasks (PySpark, serverless)
│   ├── 01_ingest_metrics_pb.py     # OTLP protobuf → bronze_metrics (custom wire-format decoder)
│   ├── 02_ingest_logs.py           # JSONL → bronze_logs (Spark SQL)
│   ├── 03_ingest_traces.py         # JSON → bronze_traces (Spark SQL)
│   ├── 04_ingest_events.py         # JSONL → bronze_incidents, bronze_alerts, bronze_topology_changes
│   ├── 05_ingest_network_flows_pb.py  # Custom NFLOW binary → bronze_network_flows
│   ├── 06_silver_transforms.py     # Bronze → 6 silver tables (enrichment, scoring, correlation)
│   └── 07_gold_transforms.py       # Silver → 5 gold tables (patterns, ranking, correlation, domain, business)
│
├── setup_pipeline/                 # One-time data setup + local pipeline scripts
│   ├── README.md                   # Script descriptions, run order, proto schema docs
│   ├── 00_create_schema_and_volume.py   # Create UC schema + managed volume with subdirs
│   ├── 01_generate_raw_telemetry.py     # Generate OTLP metrics/logs/traces/events for 5 business units
│   ├── 02_generate_protobuf_network_flows.py  # Generate network flow .pb files
│   ├── 03_create_bronze_tables.py       # Volume → bronze via databricks-sdk
│   ├── 04_create_silver_tables.py       # Bronze → silver
│   ├── 05_create_gold_tables.py         # Silver → gold
│   ├── 06_create_genie_space.py         # Create Genie Space (add tables via UI)
│   ├── 07_grant_app_uc_permissions.py   # Grant app service principal SELECT
│   ├── otlp_metrics.proto               # OTLP MetricsData wire format reference
│   └── network_flow.proto               # Custom NFLOW binary format reference
│
├── scripts/
│   └── deploy_with_env.sh          # Reads rca_app/.env, passes as databricks bundle --var overrides
│
└── images/                         # Screenshot PNGs for README/demo (one per dashboard page)
```
