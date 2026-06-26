# Plan: Externalize Business Logic into Config Tables

**Status:** Planned — ready to implement
**Created:** 2026-03-29
**Location:** Config tables in `bldemos.eo_analytics`, synced to Lakebase

## Context

Revenue impact, affected users, blast radius, and scoring weights are all hardcoded in Python dicts inside `setup_pipeline/01_generate_raw_telemetry.py` and baked into Bronze records during data generation. Silver/Gold just aggregate pre-computed values — changing a revenue rate requires modifying Python code and re-running the entire pipeline.

Moving business parameters to config tables and having **Silver compute revenue/users/blast at query time** makes the system configurable: change a rate in a table → re-run Silver/Gold → updated numbers everywhere. No code changes needed.

## Three Config Tables

### 1. `config_service_profiles` (~28 rows)

Merges SERVICES dict + SERVICE_TO_BU + BUSINESS_UNITS revenue model params.

```sql
CREATE TABLE config_service_profiles (
  service_name        STRING,       -- PK
  domain              STRING,       -- application/infrastructure/network
  tier                STRING,       -- critical/high/medium
  business_unit       STRING,       -- supply-chain, digital-surgery, etc.
  default_affected_users INT,       -- from failure patterns
  affected_roles      STRING,       -- JSON array of role names
  revenue_model       STRING,       -- shipment_throughput/productivity_loss/trial_delay/lost_sales/blast_multiplier
  revenue_rate_usd    DOUBLE,       -- primary rate (order value, hourly rate, daily cost, etc.)
  throughput_per_hour  DOUBLE,      -- shipments/hr, scientists, etc. (NULL if N/A)
  host_prefix         STRING,
  port                INT
);
```

### 2. `config_service_relationships` (~55 rows)

Flattens every `depends_on` entry into a triplet table.

```sql
CREATE TABLE config_service_relationships (
  source_service      STRING,       -- PK part 1
  target_service      STRING,       -- PK part 2
  relationship_type   STRING,       -- depends_on / impacts / network_flow
  weight              DOUBLE,       -- 1.0 default, can tune
  description         STRING        -- optional context
);
```

### 3. `config_business_impact_models` (5 rows)

Per-business-unit revenue formula parameters.

```sql
CREATE TABLE config_business_impact_models (
  business_unit             STRING,   -- PK
  impact_type               STRING,   -- shipment_throughput/productivity_loss/etc.
  revenue_per_hour_usd      DOUBLE,   -- generic hourly rate
  avg_order_value_usd       DOUBLE,   -- for shipment_throughput
  loaded_rate_per_hour_usd  DOUBLE,   -- for productivity_loss
  trial_delay_cost_per_day  DOUBLE,   -- for trial_delay
  blended_rate_per_hour     DOUBLE,   -- for blast_multiplier ($150 default)
  default_throughput_per_hr DOUBLE    -- default shipments/scientists per hour
);
```

---

## How Revenue Is Calculated (Config-Driven)

Silver transforms JOIN config tables and compute revenue at query time:

```sql
CASE bim.impact_type
  WHEN 'shipment_throughput'
    THEN sp.throughput_per_hour * (i.mttr_minutes / 60.0) * bim.avg_order_value_usd
  WHEN 'productivity_loss'
    THEN sp.default_affected_users * (i.mttr_minutes / 60.0) * bim.loaded_rate_per_hour_usd
  WHEN 'trial_delay'
    THEN GREATEST(0.25, i.mttr_minutes / 480.0) * bim.trial_delay_cost_per_day
  WHEN 'lost_sales'
    THEN (i.mttr_minutes / 60.0) * bim.revenue_per_hour_usd
  ELSE sp.default_affected_users * (i.mttr_minutes / 60.0) * bim.blended_rate_per_hour
END as revenue_impact_usd
```

**Key point**: Bronze keeps raw telemetry values, but Silver OVERRIDES revenue/users/blast with config-driven calculations.

---

## Implementation Steps

### Step 1: New script `setup_pipeline/00c_create_config_tables.py`

- Creates the 3 tables with `CREATE OR REPLACE TABLE ... USING DELTA`
- Enables Change Data Feed (`TBLPROPERTIES ('delta.enableChangeDataFeed' = true)`)
- Populates via INSERT from values extracted from the existing Python dicts in `01_generate_raw_telemetry.py`
- Follows existing `execute_sql(w, warehouse_id, sql)` pattern

### Step 2: Update `setup_pipeline/04_create_silver_tables.py`

**Key change: Silver COMPUTES revenue/users/blast from config instead of trusting Bronze values.**

- `silver_incidents`: JOIN `config_service_profiles` + `config_business_impact_models` on root_service → business_unit
  - **Revenue**: CASE on `bim.impact_type` to calculate from duration × config rates (not from `bronze_incidents.revenue_impact_usd`)
  - **Affected users**: `COALESCE(sp.default_affected_users, blast_radius * 10)`
  - **Blast radius**: Subquery counting downstream services from `config_service_relationships`
- `silver_service_health`: health_score formula stays in SQL (cross-service weights, not per-BU)

### Step 3: Update `setup_pipeline/05_create_gold_tables.py`

- `gold_service_risk_ranking`: JOIN `config_service_profiles` to add `tier`, `domain`, `business_unit` columns directly instead of the expensive domain-inference CTE
- `gold_business_impact_summary`: JOIN `config_business_impact_models` to add `revenue_model` and rate parameters as columns
- All downstream aggregations (SUM(revenue_impact_usd)) automatically pick up config-driven values from Silver

### Step 4: Update `setup_pipeline/08_setup_lakebase_sync.py`

- Add the 3 config tables to `TABLES_TO_SYNC` with primary keys:
  - `config_service_profiles`: `service_name`
  - `config_service_relationships`: `source_service, target_service, relationship_type`
  - `config_business_impact_models`: `business_unit`

### Step 5: Update `databricks.yml`

- Add `create_config_tables` task in setup job after `create_schema_and_volume`, before `generate_raw_telemetry`

### Step 6: Update topology endpoint

- `rca_app/backend/routes/service_ranking.py`: Replace the domain-inference CTE in `/topology` with a simple `LEFT JOIN config_service_profiles`
- Add `config_service_relationships` edges to the topology graph (UNION ALL with network flow edges)

### Step 7: Sync to Lakebase & grant permissions

- Run 08_setup_lakebase_sync.py with new tables
- PostgreSQL GRANTs already cover all tables in `eo_lakebase` schema

### Step 8: New Settings page in the app

**New file: `rca_app/frontend/src/pages/Settings.jsx`**

A read-only page that displays the 3 config tables and explains in plain English how the business logic works. Sections:

1. **How Revenue Impact Is Calculated** — explains the 5 business models in plain language:
   - "Supply chain revenue = shipments delayed per hour × outage duration × average order value"
   - "Digital surgery = affected data scientists × hours lost × loaded hourly rate"
   - etc.
   - Shows the current rates from `config_business_impact_models` in a table

2. **Service Profiles** — shows the `config_service_profiles` table with domain, tier, business unit, default users
   - Explains: "Affected users per incident come from the service profile. If a service has 57 default users, any incident on that service counts 57 impacted users."

3. **Service Dependencies** — shows the `config_service_relationships` table
   - Explains: "Blast radius is calculated by walking the dependency graph. If Service A depends on Service B, and Service B goes down, Service A is in the blast radius."
   - Explains: "These relationships also power the Service Map topology view."

4. **How Priority & Risk Scores Work** — explains the Gold-layer formulas:
   - Priority Score = occurrences × 2 + revenue/10K + users/10 + P1s × 20 + SLA breaches × 15 + blast × 5 + worsening trend bonus
   - Risk Score = incidents × 10 + P1s × 30 + SLA × 20 + revenue/10K + users/5 + blast × 5 + ...

**Backend**: New API endpoint `GET /api/config/tables` that returns all 3 config tables from Lakebase.

**Nav**: Add "Settings" item under an "Admin" section in the sidebar with a Settings/Cog icon.

---

## What Does NOT Change

- **`01_generate_raw_telemetry.py`**: Still uses Python dicts for synthetic data generation. Config tables are seeded FROM these values.
- **`02_generate_protobuf_network_flows.py`**, **`03_create_bronze_tables.py`**: No business logic to externalize.
- **Scoring formulas** (priority_score, risk_score): Stay as SQL — they're cross-BU ranking weights, not per-BU business parameters.

---

## Files to Modify

| File | Change |
|------|--------|
| `setup_pipeline/00c_create_config_tables.py` | **NEW** — create & populate 3 config tables |
| `setup_pipeline/04_create_silver_tables.py` | Silver computes revenue/users/blast from config JOINs |
| `setup_pipeline/05_create_gold_tables.py` | JOIN config_service_profiles in gold_service_risk_ranking |
| `setup_pipeline/08_setup_lakebase_sync.py` | Add 3 config tables to sync list |
| `databricks.yml` | Add create_config_tables task |
| `rca_app/backend/routes/service_ranking.py` | Use config tables in topology endpoint |
| `rca_app/backend/routes/config.py` | **NEW** — API endpoint for config tables |
| `rca_app/backend/main.py` | Register config router |
| `rca_app/frontend/src/pages/Settings.jsx` | **NEW** — Settings page with config tables + plain English explanations |
| `rca_app/frontend/src/App.jsx` | Add Settings nav item + route |

---

## Verification

1. Run `python setup_pipeline/00c_create_config_tables.py` — verify 3 tables created with correct row counts (28, ~55, 5)
2. Run `python setup_pipeline/04_create_silver_tables.py` — verify silver_incidents has revenue computed from config
3. Run `python setup_pipeline/05_create_gold_tables.py` — verify gold_service_risk_ranking has tier/domain from config
4. Sync to Lakebase — verify config tables appear in eo_lakebase schema
5. Start app locally — verify topology endpoint uses config_service_relationships for edges
6. Open Settings page — verify config tables display with plain English explanations
7. Verify existing dashboards still show same data (no regressions)
