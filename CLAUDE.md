# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Enterprise Root Cause Intelligence — a Databricks App that correlates OpenTelemetry signals across infrastructure, applications, and network domains for HLS (healthcare) environments. Built for a Dbrks Enterprise Observability demo.

## Repository Layout

- **`rca_app/`** — The web application (FastAPI backend + React frontend). Self-contained: `app.py`, `backend/`, `frontend/`, `requirements.txt`, `.env`, `app.yaml`.
- **`setup_pipeline/`** — All data engineering: schema/volume creation, synthetic data generation, Bronze → Silver → Gold table builds, Genie Space setup, app permissions. Run scripts 00–07 in order.
- **`data_pipelines/`** — Databricks Job notebook tasks (same Bronze → Silver → Gold logic, but runs on Spark via the Job defined in `databricks.yml`).
- **`databricks.yml`** — DABs bundle config at repo root. Defines both the App resource (`source_code_path: ./rca_app`) and the pipeline Job.

## Architecture

- **Frontend**: React 18 SPA (Vite, JSX) in `rca_app/frontend/` — Recharts for charts, Canvas for topology, Lucide icons, react-router-dom for routing
- **Backend**: FastAPI in `rca_app/backend/` — serves the API at `/api/*` and the built React SPA as static files
- **Data**: Databricks SQL Warehouse via `databricks-sdk` — queries Unity Catalog Delta tables in configurable catalog/schema (default `bldemos.eo_analytics`, Bronze/Silver/Gold medallion)
- **AI**: Databricks Foundation Model API (`databricks-claude-sonnet-4` via serving endpoint) for root cause analysis; Genie Space for natural language queries with keyword-based SQL fallback
- **Deployment**: Databricks Apps via `databricks.yml` (DABs bundle), `rca_app/app.yaml` for runtime config

### Backend Structure

- `rca_app/app.py` — Uvicorn entry point, loads `.env` via python-dotenv, imports `backend.main:app`
- `rca_app/backend/main.py` — FastAPI app setup, CORS, router registration, SPA catch-all
- `rca_app/backend/db.py` — Databricks SQL connection: reads `CATALOG`/`SCHEMA` from env vars (defaults `bldemos`/`eo_analytics`), auto-detects local dev (profile-based) vs deployed App (service principal via `DATABRICKS_APP_NAME`). Warehouse discovery prefers serverless, then running.
- `rca_app/backend/routes/` — API route modules: `incidents`, `root_cause` (includes LLM analysis with fallback), `service_ranking`, `change_correlation`, `domain_summary`, `genie` (proxies Genie Space API with SQL fallback)

### Frontend Structure

- `rca_app/frontend/src/App.jsx` — Layout with sidebar nav + react-router Routes
- `rca_app/frontend/src/pages/` — Page components: ExecutiveDashboard, RootCauseIntelligence, ServiceRiskRanking, ChangeCorrelation, DomainDeepDive, TopologyExplorer, GenieChat
- `rca_app/frontend/src/components/` — Shared UI: ChartTooltip, InfoExpander, LoadingState, SeverityBadge
- `rca_app/frontend/src/hooks/useApi.js` — Custom fetch hook for API calls
- Vite dev server proxies `/api` to `localhost:8000`

## Development Commands

### App (run from `rca_app/`)

```bash
cd rca_app

# Backend — .env is loaded automatically by app.py
pip install -r requirements.txt
python app.py                                      # Start backend on :8000

# Frontend (separate terminal)
cd frontend && npm install
cd frontend && npm run dev                         # Vite dev server on :5173 (proxies /api to :8000)
cd frontend && npm run build                       # Build to frontend/dist/ for production
```

### Data engineering (run from repo root)

```bash
# Full pipeline: schema → synthetic data → bronze → silver → gold → genie → permissions
python setup_pipeline/00_create_schema_and_volume.py
python setup_pipeline/01_generate_raw_telemetry.py
python setup_pipeline/02_generate_protobuf_network_flows.py
python setup_pipeline/03_create_bronze_tables.py
python setup_pipeline/04_create_silver_tables.py
python setup_pipeline/05_create_gold_tables.py
python setup_pipeline/06_create_genie_space.py
python setup_pipeline/07_grant_app_uc_permissions.py
```

## Environment Configuration

`rca_app/.env.example` → copy to `rca_app/.env`. Key vars: `DATABRICKS_PROFILE`, `CATALOG`, `SCHEMA`, `DATABRICKS_WAREHOUSE_ID`, `SERVING_ENDPOINT`, `GENIE_SPACE_ID`. The `.env` file is gitignored and auto-loaded by `app.py` via python-dotenv (`override=False`, so platform env vars win when deployed).

## Deployment

```bash
# Deploy via DABs with .env variable injection (from repo root)
./scripts/deploy_with_env.sh DEFAULT prod

# Or deploy directly
databricks bundle deploy --target prod
```

## Key Conventions

- `rca_app/frontend/dist/` is kept in version control (not gitignored) for deployment — always rebuild before deploying
- Local dev uses `DATABRICKS_PROFILE` env var (e.g. `DEFAULT`); deployed App auto-authenticates via service principal (detected by `DATABRICKS_APP_NAME` env var)
- `DATABRICKS_WAREHOUSE_ID` and `GENIE_SPACE_ID` are set via `app.yaml` / `databricks.yml` resource binding in production
- Route modules build SQL with f-strings using `CATALOG` and `SCHEMA` from `db.py`; the LLM endpoint and Genie Space ID come from env vars
- No test framework is configured for either frontend or backend
- `USECASE.md` contains the demo talk track and storyline for presenting to VP-level audiences
