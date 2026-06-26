"""
08 — Create Lakebase Provisioned instance and sync Delta tables.

Creates a Lakebase PostgreSQL instance, registers a database (Postgres-backed)
catalog, and sets up synced tables from the Bronze/Silver/Gold Delta tables for
low-latency app queries.

Naming model (databricks-sdk >= 0.102.0):
  - Source Delta tables live in CATALOG.SCHEMA (e.g. bldemos.eo_analytics).
  - Synced tables must target a SEPARATE database catalog (Postgres-backed),
    LAKEBASE_CATALOG, mapped to the Postgres database LAKEBASE_DATABASE.
  - A synced table named  LAKEBASE_CATALOG.SCHEMA.<table>  lands physically in
    Postgres at  <LAKEBASE_DATABASE>.<SCHEMA>.<table>  — so the app connects to
    LAKEBASE_DATABASE and sets search_path to SCHEMA (see backend/db.py).

Usage:
    python setup_pipeline/08_setup_lakebase_sync.py
"""
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────

CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME", "dbrks-eo-lb")
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
# Database (Postgres-backed) catalog that holds the synced-table targets.
# Must be a valid UC identifier (no hyphens) and distinct from the Delta CATALOG.
LAKEBASE_CATALOG = os.environ.get("LAKEBASE_CATALOG", "bldemos_lakebase")
CAPACITY = "CU_1"  # Smallest tier, sufficient for demo

# Tables to sync with their primary key columns.
# Array columns in Spark will become JSONB in PostgreSQL automatically.
TABLES_TO_SYNC = [
    # Bronze
    {"table": "bronze_metrics", "pk": ["service_name", "metric_name", "event_timestamp"], "policy": "SNAPSHOT"},
    {"table": "bronze_network_flows", "pk": ["src_service", "dst_service", "protocol", "event_timestamp"], "policy": "SNAPSHOT"},
    # Silver
    {"table": "silver_incidents", "pk": ["incident_id"], "policy": "TRIGGERED"},
    {"table": "silver_alerts", "pk": ["alert_id"], "policy": "TRIGGERED"},
    {"table": "silver_changes", "pk": ["change_id"], "policy": "TRIGGERED"},
    {"table": "silver_service_health", "pk": ["service_name", "health_date"], "policy": "TRIGGERED"},
    {"table": "silver_servicenow_correlation", "pk": ["incident_id"], "policy": "TRIGGERED"},
    # Gold
    {"table": "gold_root_cause_patterns", "pk": ["failure_pattern_id"], "policy": "TRIGGERED"},
    {"table": "gold_service_risk_ranking", "pk": ["service_name"], "policy": "TRIGGERED"},
    {"table": "gold_change_incident_correlation", "pk": ["change_id", "incident_id"], "policy": "TRIGGERED"},
    {"table": "gold_domain_impact_summary", "pk": ["domain", "summary_date"], "policy": "TRIGGERED"},
    {"table": "gold_business_impact_summary", "pk": ["business_unit"], "policy": "TRIGGERED"},
]


def get_workspace_client():
    from databricks.sdk import WorkspaceClient

    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    return WorkspaceClient(profile=profile)


def enable_cdf(w, source_table: str):
    """Enable Change Data Feed on a Delta table (required for TRIGGERED sync)."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        for wh in w.warehouses.list():
            if wh.state and wh.state.value == "RUNNING":
                warehouse_id = wh.id
                break

    sql = f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    logger.info(f"  Enabling CDF on {source_table}")
    try:
        from databricks.sdk.service.sql import StatementState

        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="30s"
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            logger.warning(f"  CDF enable may have failed: {resp.status.error}")
    except Exception as e:
        logger.warning(f"  Could not enable CDF on {source_table}: {e}")


def create_or_get_instance(w):
    """Create Lakebase instance or return existing one (started)."""
    from databricks.sdk.service.database import DatabaseInstance

    try:
        instance = w.database.get_database_instance(name=INSTANCE_NAME)
        state = str(instance.state).upper() if instance.state else "UNKNOWN"
        logger.info(f"Lakebase instance '{INSTANCE_NAME}' already exists (state: {state})")
        if "STOPPED" in state:
            logger.info("Starting stopped instance...")
            w.database.update_database_instance(
                name=INSTANCE_NAME,
                database_instance=DatabaseInstance(name=INSTANCE_NAME, stopped=False),
                update_mask="stopped",
            )
            for _ in range(60):
                time.sleep(5)
                inst = w.database.get_database_instance(name=INSTANCE_NAME)
                if inst.state and "AVAILABLE" in str(inst.state).upper():
                    break
        return w.database.get_database_instance(name=INSTANCE_NAME)
    except Exception:
        pass

    logger.info(f"Creating Lakebase instance '{INSTANCE_NAME}' with capacity {CAPACITY}...")
    instance = w.database.create_database_instance_and_wait(
        DatabaseInstance(name=INSTANCE_NAME, capacity=CAPACITY)
    )
    logger.info(f"Instance ready. DNS: {instance.read_write_dns}")
    return instance


def ensure_database_catalog(w):
    """Create the Postgres-backed database catalog that holds synced-table targets."""
    from databricks.sdk.service.database import DatabaseCatalog

    try:
        w.database.create_database_catalog(
            DatabaseCatalog(
                name=LAKEBASE_CATALOG,
                database_instance_name=INSTANCE_NAME,
                database_name=LAKEBASE_DATABASE,
                create_database_if_not_exists=True,
            )
        )
        logger.info(f"Database catalog '{LAKEBASE_CATALOG}' -> {INSTANCE_NAME}/{LAKEBASE_DATABASE} created.")
    except Exception as e:
        if "already" in str(e).lower() or "exists" in str(e).lower():
            logger.info(f"Database catalog '{LAKEBASE_CATALOG}' already exists.")
        else:
            logger.warning(f"Database catalog creation warning: {e}")


def setup_synced_tables(w):
    """Create synced tables for all configured Delta tables."""
    from databricks.sdk.service.database import (
        SyncedDatabaseTable,
        SyncedTableSpec,
        SyncedTableSchedulingPolicy,
        NewPipelineSpec,
    )

    policy_map = {
        "SNAPSHOT": SyncedTableSchedulingPolicy.SNAPSHOT,
        "TRIGGERED": SyncedTableSchedulingPolicy.TRIGGERED,
        "CONTINUOUS": SyncedTableSchedulingPolicy.CONTINUOUS,
    }

    for cfg in TABLES_TO_SYNC:
        table_name = cfg["table"]
        source_table = f"{CATALOG}.{SCHEMA}.{table_name}"
        target_table = f"{LAKEBASE_CATALOG}.{SCHEMA}.{table_name}"
        policy = cfg["policy"]

        if policy in ("TRIGGERED", "CONTINUOUS"):
            enable_cdf(w, source_table)

        # Delete any existing target first so re-runs replace failed/stale tables.
        try:
            w.database.delete_synced_database_table(name=target_table)
            logger.info(f"  Removed existing synced table: {table_name}")
            time.sleep(3)
        except Exception:
            pass

        logger.info(f"Creating synced table: {source_table} -> {target_table} ({policy})")
        try:
            w.database.create_synced_database_table(
                SyncedDatabaseTable(
                    name=target_table,
                    database_instance_name=INSTANCE_NAME,
                    logical_database_name=LAKEBASE_DATABASE,
                    spec=SyncedTableSpec(
                        source_table_full_name=source_table,
                        primary_key_columns=cfg["pk"],
                        scheduling_policy=policy_map[policy],
                        create_database_objects_if_missing=True,
                        # This metastore has no storage root; point the backing
                        # pipeline at a catalog that has managed storage.
                        new_pipeline_spec=NewPipelineSpec(
                            storage_catalog=CATALOG,
                            storage_schema=SCHEMA,
                        ),
                    ),
                )
            )
            logger.info(f"  ✓ Synced table created: {table_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"  ✓ Synced table already exists: {table_name}")
            else:
                logger.error(f"  ✗ Failed to sync {table_name}: {e}")

    logger.info("Synced table setup complete.")


def wait_for_initial_sync(w):
    """Wait for all synced tables to complete their initial sync."""
    logger.info("Waiting for initial sync to complete...")
    pending = set(t["table"] for t in TABLES_TO_SYNC)
    for attempt in range(60):
        still_pending = set()
        for table_name in pending:
            full_name = f"{LAKEBASE_CATALOG}.{SCHEMA}.{table_name}"
            try:
                status = w.database.get_synced_database_table(name=full_name)
                sync_status = status.data_synchronization_status
                state = str(sync_status.detailed_state).upper() if sync_status and sync_status.detailed_state else ""
                if any(s in state for s in ("ACTIVE", "ONLINE", "SUCCEEDED")):
                    logger.info(f"  ✓ {table_name} sync complete")
                    continue
                still_pending.add(table_name)
            except Exception:
                still_pending.add(table_name)

        pending = still_pending
        if not pending:
            logger.info("All tables synced successfully!")
            return
        if attempt % 6 == 0:
            logger.info(f"  {len(pending)} tables still syncing: {', '.join(sorted(pending))}")
        time.sleep(10)

    if pending:
        logger.warning(f"Some tables may still be syncing: {', '.join(sorted(pending))}")


def print_summary(instance):
    host = instance.read_write_dns if instance.read_write_dns else "<pending>"
    print("\n" + "=" * 60)
    print("Lakebase setup complete!")
    print("=" * 60)
    print(f"\nAdd these to rca_app/.env:\n")
    print(f"LAKEBASE_HOST={host}")
    print(f"LAKEBASE_DATABASE={LAKEBASE_DATABASE}")
    print(f"LAKEBASE_INSTANCE_NAME={INSTANCE_NAME}")
    print(f"\nSynced tables live in Postgres {LAKEBASE_DATABASE}.{SCHEMA}.* "
          f"(UC catalog: {LAKEBASE_CATALOG})")
    print(f"{'=' * 60}\n")


def main():
    logger.info("=" * 60)
    logger.info("Lakebase Sync Setup for EO Analytics")
    logger.info(f"  Source catalog: {CATALOG}")
    logger.info(f"  Schema:         {SCHEMA}")
    logger.info(f"  Instance:       {INSTANCE_NAME}")
    logger.info(f"  DB catalog:     {LAKEBASE_CATALOG} -> {LAKEBASE_DATABASE}")
    logger.info(f"  Tables:         {len(TABLES_TO_SYNC)}")
    logger.info("=" * 60)

    w = get_workspace_client()
    instance = create_or_get_instance(w)
    ensure_database_catalog(w)
    setup_synced_tables(w)
    wait_for_initial_sync(w)
    print_summary(instance)


if __name__ == "__main__":
    main()
