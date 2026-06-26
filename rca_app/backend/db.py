"""
Database connection module for Lakebase PostgreSQL (primary) and Databricks SQL Warehouse (Genie fallback).
Supports both local development (OAuth credential generation) and deployed App (service principal).
"""
import os
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Lakebase connection settings
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
LAKEBASE_INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME", "dbrks-eo-lb")
# Synced tables (script 08) land in the Postgres schema matching the UC schema.
# Defaults to SCHEMA; override with PG_SCHEMA only if tables are synced elsewhere.
PG_SCHEMA = os.environ.get("PG_SCHEMA", SCHEMA)


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient with proper auth for environment."""
    if IS_DATABRICKS_APP:
        try:
            return WorkspaceClient(auth_type="oauth-m2m")
        except Exception:
            return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host


def get_oauth_token() -> Optional[str]:
    """Get OAuth token for API calls."""
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    return None


# ── Lakebase PostgreSQL connection ───────────────────────────────────────────

_engine: Optional[Engine] = None


def _inject_credential(dialect, conn_rec, cargs, cparams):
    """SQLAlchemy do_connect event: inject fresh OAuth token as password."""
    w = get_workspace_client()
    cred = w.database.generate_database_credential(
        instance_names=[LAKEBASE_INSTANCE_NAME]
    )
    cparams["password"] = cred.token


def get_engine() -> Engine:
    """Get or create the SQLAlchemy engine for Lakebase PostgreSQL.

    Both local dev and deployed app use OAuth credential injection.
    The Lakebase resource binding gives the SP permission to generate credentials.
    We always connect to the shared databricks_postgres database where synced tables live.
    """
    global _engine
    if _engine is not None:
        return _engine

    # Resolve host
    host = LAKEBASE_HOST
    if not host:
        w = get_workspace_client()
        instance = w.database.get_database_instance(LAKEBASE_INSTANCE_NAME)
        host = instance.read_write_dns

    # Resolve username (SP client_id when deployed, user email for local dev)
    w = get_workspace_client()
    username = w.config.client_id if w.config.client_id else w.current_user.me().user_name
    database = LAKEBASE_DATABASE
    port = os.environ.get("PGPORT", "5432")

    logger.info(f"Connecting to Lakebase at {host} db={database} user={username}")

    url = f"postgresql+psycopg://{username}:@{host}:{port}/{database}"
    engine = create_engine(
        url,
        pool_recycle=45 * 60,
        pool_size=4,
        pool_pre_ping=True,
        connect_args={"sslmode": "require"},
    )
    event.listen(engine, "do_connect", _inject_credential)

    _engine = engine
    return engine


def execute_query(sql: str, params: dict = None) -> list[dict]:
    """Execute SQL against Lakebase PostgreSQL and return results as list of dicts."""
    engine = get_engine()

    if params:
        for key, value in params.items():
            if isinstance(value, str):
                sql = sql.replace(f":{key}", f"'{value}'")
            else:
                sql = sql.replace(f":{key}", str(value))

    try:
        with engine.connect() as conn:
            # Ensure search_path includes our synced table schema
            conn.execute(text(f"SET search_path TO {PG_SCHEMA}, public"))
            result = conn.execute(text(sql))
            columns = list(result.keys())
            rows = []
            for row in result:
                record = {}
                for i, col_name in enumerate(columns):
                    val = row[i]
                    # Serialize PostgreSQL types to JSON-safe values
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    elif isinstance(val, date):
                        val = val.isoformat()
                    elif isinstance(val, Decimal):
                        val = float(val)
                    record[col_name] = val
                rows.append(record)
            return rows
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise


# ── SQL Warehouse connection (for Genie Space only) ─────────────────────────

_warehouse_id_cache = None


def get_warehouse_id() -> str:
    """Find a running SQL warehouse (used by Genie Space)."""
    global _warehouse_id_cache
    if _warehouse_id_cache:
        return _warehouse_id_cache

    env_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env_wh:
        _warehouse_id_cache = env_wh
        return env_wh

    w = get_workspace_client()
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING" and wh.enable_serverless_compute:
            _warehouse_id_cache = wh.id
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            _warehouse_id_cache = wh.id
            return wh.id
    if warehouses:
        _warehouse_id_cache = warehouses[0].id
        return warehouses[0].id
    raise RuntimeError("No SQL warehouse found")
