"""
03_create_gold_tables.py
Creates Gold (analytics-ready) Delta tables:
  - gold_root_cause_patterns
  - gold_service_risk_ranking
  - gold_change_incident_correlation
  - gold_domain_impact_summary
  - gold_business_impact_summary  (NEW - per business unit)
"""
import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")


def get_warehouse_id(w):
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING",) and wh.enable_serverless_compute:
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING",):
            return wh.id
    if warehouses:
        wh = warehouses[0]
        w.warehouses.start(wh.id)
        time.sleep(30)
        return wh.id
    raise RuntimeError("No SQL warehouse found.")


def execute_sql(w, warehouse_id, sql, description=""):
    if description:
        print(f"  {description} ...")
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        catalog=CATALOG,
        schema=SCHEMA,
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        return resp
    elif resp.status and resp.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    else:
        stmt_id = resp.statement_id
        for _ in range(120):
            time.sleep(5)
            status = w.statement_execution.get_statement(stmt_id)
            if status.status.state == StatementState.SUCCEEDED:
                return status
            if status.status.state == StatementState.FAILED:
                raise RuntimeError(f"SQL failed: {status.status.error}")
        raise RuntimeError("SQL timed out")


def main():
    w = WorkspaceClient(profile=PROFILE)
    warehouse_id = get_warehouse_id(w)

    print(f"Creating Gold tables in {CATALOG}.{SCHEMA} ...")

    # ── gold_root_cause_patterns ───────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_root_cause_patterns AS
    WITH impacted_services_agg AS (
      SELECT
        failure_pattern_id,
        collect_set(svc) as all_impacted_services
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      LATERAL VIEW explode(impacted_services) explode_svc AS svc
      WHERE failure_pattern_id IS NOT NULL
      GROUP BY failure_pattern_id
    ),
    pattern_stats AS (
      SELECT
        failure_pattern_id,
        failure_pattern_name,
        root_service,
        domain,
        first(business_unit) as business_unit,
        COUNT(*) as occurrence_count,
        AVG(mttr_minutes) as avg_mttr_minutes,
        MAX(mttr_minutes) as max_mttr_minutes,
        MIN(mttr_minutes) as min_mttr_minutes,
        PERCENTILE(mttr_minutes, 0.5) as p50_mttr_minutes,
        PERCENTILE(mttr_minutes, 0.95) as p95_mttr_minutes,
        AVG(blast_radius) as avg_blast_radius,
        MAX(blast_radius) as max_blast_radius,
        SUM(revenue_impact_usd) as total_revenue_impact,
        AVG(revenue_impact_usd) as avg_revenue_impact,
        SUM(affected_user_count) as total_affected_users,
        AVG(affected_user_count) as avg_affected_users,
        SUM(servicenow_ticket_count) as total_servicenow_tickets,
        SUM(servicenow_duplicate_tickets) as total_duplicate_tickets,
        SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
        SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
        SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
        SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breach_count,
        MIN(created_at) as first_occurrence,
        MAX(created_at) as last_occurrence,
        first(revenue_model) as revenue_model,
        first(root_cause_explanation) as root_cause_explanation,
        collect_set(root_service) as affected_root_services
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      WHERE failure_pattern_id IS NOT NULL
      GROUP BY failure_pattern_id, failure_pattern_name, root_service, domain
    ),
    weekly_trend AS (
      SELECT
        failure_pattern_id,
        WEEKOFYEAR(created_at) as week_num,
        YEAR(created_at) as year_num,
        COUNT(*) as weekly_count
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      WHERE failure_pattern_id IS NOT NULL
      GROUP BY failure_pattern_id, WEEKOFYEAR(created_at), YEAR(created_at)
    ),
    trend_summary AS (
      SELECT
        failure_pattern_id,
        AVG(CASE WHEN (year_num * 52 + week_num) >= ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 4)
          THEN weekly_count END) as recent_avg,
        AVG(CASE WHEN (year_num * 52 + week_num) BETWEEN
          ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 8)
          AND ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 4)
          THEN weekly_count END) as previous_avg
      FROM weekly_trend
      GROUP BY failure_pattern_id
    )
    SELECT
      ps.*,
      isa.all_impacted_services,
      CASE
        WHEN ts.recent_avg > ts.previous_avg * 1.2 THEN 'worsening'
        WHEN ts.recent_avg < ts.previous_avg * 0.8 THEN 'improving'
        ELSE 'stable'
      END as trend_direction,
      ROUND(ts.recent_avg, 2) as recent_weekly_avg,
      ROUND(ts.previous_avg, 2) as previous_weekly_avg,
      ROUND(
        ps.occurrence_count * 2.0
        + ps.total_revenue_impact / 10000.0
        + ps.total_affected_users / 10.0
        + ps.p1_count * 20.0
        + ps.sla_breach_count * 15.0
        + ps.avg_blast_radius * 5.0
        + CASE WHEN ts.recent_avg > ts.previous_avg * 1.2 THEN 50 ELSE 0 END
      , 2) as priority_score,
      CASE WHEN ps.occurrence_count > 1
        THEN ROUND(DATEDIFF(ps.last_occurrence, ps.first_occurrence) / (ps.occurrence_count - 1.0), 1)
        ELSE NULL
      END as avg_days_between_occurrences,
      current_timestamp() as computed_at
    FROM pattern_stats ps
    LEFT JOIN impacted_services_agg isa ON ps.failure_pattern_id = isa.failure_pattern_id
    LEFT JOIN trend_summary ts ON ps.failure_pattern_id = ts.failure_pattern_id
    ORDER BY priority_score DESC
    """, "Creating gold_root_cause_patterns")

    # ── gold_service_risk_ranking ──────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_service_risk_ranking AS
    WITH incident_stats AS (
      SELECT
        root_service as service_name,
        first(business_unit) as business_unit,
        COUNT(*) as incident_count,
        SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
        SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
        SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
        AVG(mttr_minutes) as avg_mttr,
        SUM(blast_radius) as total_blast_radius,
        AVG(blast_radius) as avg_blast_radius,
        SUM(revenue_impact_usd) as total_revenue_impact,
        SUM(affected_user_count) as total_affected_users,
        SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breaches,
        COUNT(DISTINCT failure_pattern_id) as unique_failure_patterns,
        AVG(impact_score) as avg_impact_score
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      GROUP BY root_service
    ),
    impacted_stats AS (
      SELECT
        svc as service_name,
        COUNT(*) as times_impacted
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      LATERAL VIEW explode(impacted_services) t AS svc
      GROUP BY svc
    ),
    health_stats AS (
      SELECT
        service_name,
        AVG(health_score) as avg_health_score,
        MIN(health_score) as min_health_score,
        AVG(error_rate_pct) as avg_error_rate,
        AVG(avg_cpu_pct) as avg_cpu
      FROM {CATALOG}.{SCHEMA}.silver_service_health
      GROUP BY service_name
    ),
    change_stats AS (
      SELECT
        service,
        COUNT(*) as total_changes,
        SUM(incidents_within_4h) as changes_followed_by_incidents
      FROM {CATALOG}.{SCHEMA}.silver_changes
      GROUP BY service
    )
    SELECT
      COALESCE(ist.service_name, imp.service_name, hs.service_name) as service_name,
      COALESCE(ist.business_unit, 'shared-infrastructure') as business_unit,
      COALESCE(ist.incident_count, 0) as incident_count_as_root,
      COALESCE(imp.times_impacted, 0) as times_impacted_by_others,
      COALESCE(ist.p1_count, 0) as p1_count,
      COALESCE(ist.p2_count, 0) as p2_count,
      COALESCE(ist.p3_count, 0) as p3_count,
      COALESCE(ist.avg_mttr, 0) as avg_mttr_minutes,
      COALESCE(ist.total_blast_radius, 0) as total_blast_radius,
      COALESCE(ist.avg_blast_radius, 0) as avg_blast_radius,
      COALESCE(ist.total_revenue_impact, 0) as total_revenue_impact,
      COALESCE(ist.total_affected_users, 0) as total_affected_users,
      COALESCE(ist.sla_breaches, 0) as sla_breaches,
      COALESCE(ist.unique_failure_patterns, 0) as unique_failure_patterns,
      COALESCE(hs.avg_health_score, 100) as avg_health_score,
      COALESCE(hs.min_health_score, 100) as min_health_score,
      COALESCE(hs.avg_error_rate, 0) as avg_error_rate,
      COALESCE(hs.avg_cpu, 0) as avg_cpu_utilization,
      COALESCE(cs.total_changes, 0) as total_changes,
      COALESCE(cs.changes_followed_by_incidents, 0) as risky_changes,
      ROUND(
        COALESCE(ist.incident_count, 0) * 10.0
        + COALESCE(ist.p1_count, 0) * 30.0
        + COALESCE(ist.sla_breaches, 0) * 20.0
        + COALESCE(ist.total_revenue_impact, 0) / 10000.0
        + COALESCE(ist.total_affected_users, 0) / 5.0
        + COALESCE(ist.avg_blast_radius, 0) * 5.0
        + COALESCE(imp.times_impacted, 0) * 2.0
        + (100 - COALESCE(hs.avg_health_score, 100)) * 0.5
        + COALESCE(cs.changes_followed_by_incidents, 0) * 8.0
      , 2) as risk_score,
      ROW_NUMBER() OVER (ORDER BY
        COALESCE(ist.incident_count, 0) * 10.0
        + COALESCE(ist.p1_count, 0) * 30.0
        + COALESCE(ist.total_revenue_impact, 0) / 10000.0
        + COALESCE(ist.total_affected_users, 0) / 5.0
        DESC
      ) as risk_rank,
      current_timestamp() as computed_at
    FROM incident_stats ist
    FULL OUTER JOIN impacted_stats imp ON ist.service_name = imp.service_name
    FULL OUTER JOIN health_stats hs ON COALESCE(ist.service_name, imp.service_name) = hs.service_name
    LEFT JOIN change_stats cs ON COALESCE(ist.service_name, imp.service_name) = cs.service
    ORDER BY risk_score DESC
    """, "Creating gold_service_risk_ranking")

    # ── gold_change_incident_correlation ───────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_change_incident_correlation AS
    WITH change_incident_pairs AS (
      SELECT
        c.change_id,
        c.service as change_service,
        c.change_type,
        c.risk_level,
        c.risk_score,
        c.executed_at as change_time,
        c.executed_by,
        i.incident_id,
        i.severity as incident_severity,
        i.title as incident_title,
        i.root_service as incident_root_service,
        i.business_unit,
        i.created_at as incident_time,
        i.mttr_minutes,
        i.blast_radius,
        i.revenue_impact_usd,
        i.failure_pattern_id,
        TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) as minutes_between,
        CASE
          WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 30 THEN 'immediate'
          WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 120 THEN 'short_delay'
          WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 480 THEN 'delayed'
          ELSE 'long_delay'
        END as correlation_window
      FROM {CATALOG}.{SCHEMA}.silver_changes c
      JOIN {CATALOG}.{SCHEMA}.silver_incidents i
        ON i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 24 HOURS
        AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
    ),
    change_type_stats AS (
      SELECT
        change_type,
        COUNT(DISTINCT change_id) as total_changes,
        COUNT(DISTINCT incident_id) as incidents_caused,
        ROUND(COUNT(DISTINCT incident_id) * 100.0 / NULLIF(COUNT(DISTINCT change_id), 0), 2) as incident_rate_pct,
        AVG(minutes_between) as avg_time_to_incident,
        SUM(revenue_impact_usd) as total_impact_usd,
        SUM(blast_radius) as total_blast_radius
      FROM change_incident_pairs
      GROUP BY change_type
    )
    SELECT
      cip.*,
      ROUND(
        CASE
          WHEN cip.correlation_window = 'immediate' THEN 0.9
          WHEN cip.correlation_window = 'short_delay' THEN 0.7
          WHEN cip.correlation_window = 'delayed' THEN 0.4
          ELSE 0.2
        END
        * CASE WHEN cip.change_service = cip.incident_root_service THEN 1.5 ELSE 1.0 END
        * cip.risk_score / 3.0
      , 3) as correlation_strength,
      cts.incident_rate_pct as change_type_incident_rate,
      cts.total_changes as change_type_total_count,
      current_timestamp() as computed_at
    FROM change_incident_pairs cip
    LEFT JOIN change_type_stats cts ON cip.change_type = cts.change_type
    ORDER BY correlation_strength DESC
    """, "Creating gold_change_incident_correlation")

    # ── gold_domain_impact_summary ─────────────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_domain_impact_summary AS
    WITH domain_incidents AS (
      SELECT
        domain,
        DATE(created_at) as incident_date,
        MONTH(created_at) as incident_month,
        YEAR(created_at) as incident_year,
        COUNT(*) as incident_count,
        SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
        SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
        SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
        AVG(mttr_minutes) as avg_mttr,
        SUM(blast_radius) as total_blast_radius,
        SUM(revenue_impact_usd) as total_revenue_impact,
        SUM(affected_user_count) as total_affected_users,
        SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breaches,
        collect_set(root_service) as affected_services,
        collect_set(failure_pattern_id) as failure_patterns
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      GROUP BY domain, DATE(created_at), MONTH(created_at), YEAR(created_at)
    ),
    domain_alerts AS (
      SELECT
        domain,
        DATE(fired_at) as alert_date,
        COUNT(*) as alert_count,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_alert_count,
        SUM(CASE WHEN is_pre_incident_signal THEN 1 ELSE 0 END) as pre_incident_signals
      FROM {CATALOG}.{SCHEMA}.silver_alerts
      GROUP BY domain, DATE(fired_at)
    ),
    domain_changes AS (
      SELECT
        domain,
        DATE(executed_at) as change_date,
        COUNT(*) as change_count,
        SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_changes,
        SUM(incidents_within_4h) as changes_causing_incidents
      FROM {CATALOG}.{SCHEMA}.silver_changes
      GROUP BY domain, DATE(executed_at)
    )
    SELECT
      COALESCE(di.domain, da.domain, dc.domain) as domain,
      COALESCE(di.incident_date, da.alert_date, dc.change_date) as summary_date,
      COALESCE(di.incident_month, MONTH(da.alert_date), MONTH(dc.change_date)) as summary_month,
      COALESCE(di.incident_year, YEAR(da.alert_date), YEAR(dc.change_date)) as summary_year,
      COALESCE(di.incident_count, 0) as incident_count,
      COALESCE(di.p1_count, 0) as p1_count,
      COALESCE(di.p2_count, 0) as p2_count,
      COALESCE(di.p3_count, 0) as p3_count,
      COALESCE(di.avg_mttr, 0) as avg_mttr_minutes,
      COALESCE(di.total_blast_radius, 0) as total_blast_radius,
      COALESCE(di.total_revenue_impact, 0) as total_revenue_impact,
      COALESCE(di.total_affected_users, 0) as total_affected_users,
      COALESCE(di.sla_breaches, 0) as sla_breaches,
      COALESCE(da.alert_count, 0) as alert_count,
      COALESCE(da.critical_alert_count, 0) as critical_alert_count,
      COALESCE(da.pre_incident_signals, 0) as pre_incident_signals,
      COALESCE(dc.change_count, 0) as change_count,
      COALESCE(dc.high_risk_changes, 0) as high_risk_changes,
      COALESCE(dc.changes_causing_incidents, 0) as changes_causing_incidents,
      ROUND(
        COALESCE(di.incident_count, 0) * 10
        + COALESCE(di.p1_count, 0) * 25
        + COALESCE(di.total_revenue_impact, 0) / 5000
        + COALESCE(di.total_affected_users, 0) / 2
        + COALESCE(di.sla_breaches, 0) * 15
        + COALESCE(dc.changes_causing_incidents, 0) * 8
      , 2) as domain_risk_score,
      current_timestamp() as computed_at
    FROM domain_incidents di
    FULL OUTER JOIN domain_alerts da
      ON di.domain = da.domain AND di.incident_date = da.alert_date
    FULL OUTER JOIN domain_changes dc
      ON COALESCE(di.domain, da.domain) = dc.domain
      AND COALESCE(di.incident_date, da.alert_date) = dc.change_date
    ORDER BY domain_risk_score DESC
    """, "Creating gold_domain_impact_summary")

    # ── gold_business_impact_summary (NEW) ─────────────────────────
    execute_sql(w, warehouse_id, f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_business_impact_summary AS
    SELECT
      i.business_unit,
      COUNT(*) as total_incidents,
      SUM(CASE WHEN i.severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
      SUM(CASE WHEN i.severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
      ROUND(AVG(i.mttr_minutes), 1) as avg_mttr_minutes,
      ROUND(SUM(i.revenue_impact_usd), 2) as total_revenue_impact,
      first(i.revenue_model) as primary_revenue_model,
      SUM(i.affected_user_count) as total_affected_users,
      SUM(i.productivity_loss_usd) as total_productivity_loss,
      SUM(i.shipments_delayed) as total_shipments_delayed,
      SUM(i.servicenow_ticket_count) as total_servicenow_tickets,
      SUM(i.servicenow_duplicate_tickets) as total_duplicate_tickets,
      ROUND(SUM(i.servicenow_duplicate_tickets) * 100.0 / NULLIF(SUM(i.servicenow_ticket_count), 0), 1) as overall_duplicate_pct,
      ROUND(AVG(i.blast_radius), 1) as avg_blast_radius,
      SUM(CASE WHEN i.sla_breached THEN 1 ELSE 0 END) as sla_breaches,
      COUNT(DISTINCT i.failure_pattern_id) as unique_failure_patterns,
      COUNT(DISTINCT i.root_service) as affected_services_count,
      current_timestamp() as computed_at
    FROM {CATALOG}.{SCHEMA}.silver_incidents i
    GROUP BY i.business_unit
    ORDER BY total_revenue_impact DESC
    """, "Creating gold_business_impact_summary")

    # Verify
    for table in ["gold_root_cause_patterns", "gold_service_risk_ranking",
                   "gold_change_incident_correlation", "gold_domain_impact_summary",
                   "gold_business_impact_summary"]:
        resp = execute_sql(w, warehouse_id, f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{table}")
        if resp.result and resp.result.data_array:
            count = resp.result.data_array[0][0]
            print(f"  {table}: {count} rows")

    print("\nGold tables created successfully.")


if __name__ == "__main__":
    main()
