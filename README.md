# Enterprise Root Cause Intelligence

**Correlating signals across domains and time to reveal systemic causes, not just incidents.**

An enterprise-wide observability platform built on Databricks, ingesting OpenTelemetry signals across infrastructure, applications, and network domains for healthcare/HLS environments.

## Architecture

- **Frontend**: React 18 SPA (Vite) with Recharts/Canvas visualizations — lives in `rca_app/frontend/`
- **Backend**: FastAPI serving `/api/*` endpoints — lives in `rca_app/backend/`
- **Data**: Unity Catalog Delta tables in `bldemos.eo_analytics` (Bronze → Silver → Gold medallion)
- **AI**: Databricks Foundation Model API for explainable root cause analysis; Genie Space for natural language Q&A
- **Deployment**: Databricks Apps via DABs (`databricks.yml`)

## Prerequisites

- Python 3.10+ with a virtualenv
- Node.js 18+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) configured with a profile (default: `DEFAULT`)

## Quick Start

### 1. Set up environment variables

```bash
cp rca_app/.env.example rca_app/.env
```

Edit `rca_app/.env` with your values:

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_PROFILE` | Yes | Databricks CLI profile name from `~/.databrickscfg` |
| `CATALOG` | Yes | Unity Catalog catalog name (e.g. `bldemos`) |
| `SCHEMA` | Yes | Schema name (e.g. `eo_analytics`) |
| `DATABRICKS_WAREHOUSE_ID` | No | SQL Warehouse ID — auto-discovers a running serverless warehouse if blank |
| `SERVING_ENDPOINT` | No | Foundation Model API endpoint name (default: `databricks-claude-sonnet-4`) |
| `GENIE_SPACE_ID` | No | Genie Space ID for natural language Q&A — falls back to SQL if blank |
| `APP_NAME` | No | App name for deployment (default: `dbrks-eo-analytics-demo`) |
| `VOLUME` | No | Volume name for raw data landing (default: `raw_landing`) |
| `DATABRICKS_TOKEN` | No | Only needed if not using profile-based auth |

### 2. Data setup (synthetic data)

Create the schema/volume, optionally load checked-in static data, and generate synthetic data into the Unity Catalog volume (scripts skip if volume already has data):

```bash
python setup_pipeline/00_create_schema_and_volume.py
python setup_pipeline/00b_load_static_data.py
python setup_pipeline/01_generate_raw_telemetry.py
python setup_pipeline/02_generate_protobuf_network_flows.py
```

### 3. Pipeline (volume → bronze → silver → gold)

Build the medallion tables, configure Genie, and grant app permissions:

```bash
python setup_pipeline/03_create_bronze_tables.py
python setup_pipeline/04_create_silver_tables.py
python setup_pipeline/05_create_gold_tables.py
python setup_pipeline/06_create_genie_space.py
python setup_pipeline/07_grant_app_uc_permissions.py
```

See `setup_pipeline/README.md` for full details and proto schema documentation.

### 4. Run locally

```bash
# Install backend dependencies
cd rca_app && pip install -r requirements.txt

# Install and build frontend
cd frontend && npm install && npm run build && cd ..

# Start the server (serves API + built frontend on :8000)
python app.py
```

For frontend development with hot reload, run the backend and Vite dev server separately:

```bash
# Terminal 1 — backend
cd rca_app && python app.py

# Terminal 2 — frontend (proxies /api → localhost:8000)
cd rca_app/frontend && npm run dev
```

### 5. Deploy to Databricks

```bash
# Deploy the app + pipeline job via DABs
databricks bundle deploy --profile DEFAULT

# Optional: run one-time setup orchestration job (schema/volume + medallion build)
databricks bundle run dbrks-eo-analytics-demo-setup --profile DEFAULT

# Kick off the data pipeline
databricks bundle run dbrks-eo-analytics-demo-pipeline --profile DEFAULT
```

Or use the helper script which injects `.env` values as DAB variable overrides:

```bash
./scripts/deploy_with_env.sh DEFAULT
```

## Data Model

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze_metrics` | Parsed OTLP metrics from protobuf |
| Bronze | `bronze_logs` | Parsed structured logs from JSONL |
| Bronze | `bronze_traces` | Parsed distributed trace spans |
| Bronze | `bronze_incidents` | Raw incident records |
| Bronze | `bronze_alerts` | Raw alert records |
| Bronze | `bronze_topology_changes` | Raw change events |
| Bronze | `bronze_network_flows` | Parsed network flow data from protobuf |
| Silver | `silver_incidents` | Enriched with correlated alerts, changes, and impact scoring |
| Silver | `silver_alerts` | Correlated with incident linkage and breach analysis |
| Silver | `silver_changes` | Risk-scored changes with incident correlation |
| Silver | `silver_service_health` | Per-service daily composite health scores |
| Silver | `silver_business_impact` | Revenue/productivity impact classification |
| Silver | `silver_servicenow_correlation` | ServiceNow ticket dedup analysis |
| Gold | `gold_root_cause_patterns` | Recurring failure pattern signatures with trend detection |
| Gold | `gold_service_risk_ranking` | Services ranked by composite risk score |
| Gold | `gold_change_incident_correlation` | Change-incident causal analysis with correlation strength |
| Gold | `gold_domain_impact_summary` | Domain-level daily impact aggregation |
| Gold | `gold_business_impact_summary` | Per-business-unit financial impact summary |

## Key Features

1. **Executive Dashboard** — Top systemic issues, domain risk heatmap, business impact trend
2. **Root Cause Intelligence** — AI-powered pattern detection with explainable insights
3. **Service Risk Ranking** — Ranked by incident frequency, blast radius, business impact
4. **Change Correlation** — Timeline showing changes vs incidents with correlation strength
5. **Domain Deep Dive** — Per-domain incident and alert explorer
6. **Topology Explorer** — Canvas-based service dependency graph with failure propagation
7. **Ask Genie** — Natural language Q&A powered by Databricks Genie Space
