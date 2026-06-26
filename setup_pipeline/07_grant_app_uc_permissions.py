"""
05_grant_app_uc_permissions.py
Grant Unity Catalog read permissions to a Databricks App service principal.

Usage:
  DATABRICKS_PROFILE=DEFAULT python setup/05_grant_app_uc_permissions.py

Optional env vars:
  APP_NAME=dbrks-eo-analytics-demo
  CATALOG=bldemos
  SCHEMA=eo_analytics
  DATABRICKS_WAREHOUSE_ID=<warehouse_id>
"""
import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
APP_NAME = os.environ.get("APP_NAME", "dbrks-eo-analytics-demo")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")


def escape_ident(name: str) -> str:
    return name.replace("`", "``")


def parse_rows(resp) -> list[list]:
    if not resp.result or not resp.result.data_array:
        return []
    return resp.result.data_array


def execute_sql(w: WorkspaceClient, warehouse_id: str, statement: str):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        return resp
    if resp.status and resp.status.state == StatementState.FAILED:
        raise RuntimeError(resp.status.error.message if resp.status.error else "SQL failed")

    stmt_id = resp.statement_id
    for _ in range(60):
        time.sleep(2)
        status = w.statement_execution.get_statement(stmt_id)
        if status.status.state == StatementState.SUCCEEDED:
            return status
        if status.status.state == StatementState.FAILED:
            raise RuntimeError(status.status.error.message if status.status.error else "SQL failed")
    raise RuntimeError("SQL timed out")


def get_warehouse_id(w: WorkspaceClient) -> str:
    env_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env_wh:
        return env_wh

    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING" and wh.enable_serverless_compute:
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            return wh.id
    if warehouses:
        return warehouses[0].id
    raise RuntimeError("No SQL warehouse found")


def get_app_principal_candidates(w: WorkspaceClient, app_name: str) -> list[str]:
    app = w.apps.get(name=app_name)
    candidates = []
    for value in [
        app.service_principal_name,
        app.service_principal_client_id,
        str(app.service_principal_id) if app.service_principal_id else None,
        app.oauth2_app_client_id,
        app.oauth2_app_integration_id,
    ]:
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def principal_exists_for_grants(w: WorkspaceClient, warehouse_id: str, principal: str) -> bool:
    stmt = f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{escape_ident(principal)}`"
    try:
        execute_sql(w, warehouse_id, stmt)
        return True
    except Exception as e:
        msg = str(e)
        if "PRINCIPAL_DOES_NOT_EXIST" in msg or "Could not find principal" in msg:
            return False
        raise


def grant_permissions(w: WorkspaceClient, warehouse_id: str, principal: str):
    principal_ident = f"`{escape_ident(principal)}`"

    print(f"Granting catalog/schema access to {principal} ...")
    execute_sql(w, warehouse_id, f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {principal_ident}")
    execute_sql(w, warehouse_id, f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO {principal_ident}")

    print(f"Granting SELECT on existing tables/views in {CATALOG}.{SCHEMA} ...")
    info_sql = (
        f"SELECT table_name, table_type FROM {CATALOG}.information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}'"
    )
    rows = parse_rows(execute_sql(w, warehouse_id, info_sql))

    table_count = 0
    view_count = 0
    for row in rows:
        table_name = row[0]
        table_type = (row[1] or "").upper()
        if table_type == "VIEW":
            execute_sql(
                w,
                warehouse_id,
                f"GRANT SELECT ON VIEW {CATALOG}.{SCHEMA}.{table_name} TO {principal_ident}",
            )
            view_count += 1
        else:
            execute_sql(
                w,
                warehouse_id,
                f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.{table_name} TO {principal_ident}",
            )
            table_count += 1

    print(f"Granted SELECT on {table_count} table(s) and {view_count} view(s).")


def main():
    w = WorkspaceClient(profile=PROFILE)
    warehouse_id = get_warehouse_id(w)

    print(f"Profile: {PROFILE}")
    print(f"App: {APP_NAME}")
    print(f"Target: {CATALOG}.{SCHEMA}")
    print(f"Warehouse: {warehouse_id}")

    candidates = get_app_principal_candidates(w, APP_NAME)
    if not candidates:
        raise RuntimeError(f"No app principal candidates found for app '{APP_NAME}'")

    print("Testing principal candidates for GRANT compatibility ...")
    selected = None
    for candidate in candidates:
        print(f"  Trying: {candidate}")
        if principal_exists_for_grants(w, warehouse_id, candidate):
            selected = candidate
            print(f"  Selected principal: {selected}")
            break

    if not selected:
        raise RuntimeError(
            "Could not resolve a grantable principal. "
            f"Tried: {', '.join(candidates)}"
        )

    grant_permissions(w, warehouse_id, selected)
    print("\nDone. App principal should now be able to query Unity Catalog tables.")


if __name__ == "__main__":
    main()
