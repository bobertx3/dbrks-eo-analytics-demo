# Databricks notebook source
# MAGIC %sql
# MAGIC -- Silver Layer Transforms for Enterprise RCA Intelligence Pipeline
# MAGIC -- Creates 6 enriched silver tables from bronze data
# MAGIC -- Run against: bldemos.eo_analytics
# MAGIC --
# MAGIC -- Updated for Dbrks business unit schema with:
# MAGIC --   business_unit, affected_user_count, affected_roles, productivity_loss_*,
# MAGIC --   shipments_delayed, servicenow_ticket_count/duplicate_tickets,
# MAGIC --   downstream_impact_narrative, root_cause_explanation, revenue_model
# MAGIC -- New table: silver_servicenow_correlation
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_incidents: Enriched incidents with business context and impact scoring
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_incidents AS
# MAGIC SELECT
# MAGIC   i.incident_id,
# MAGIC   i.title,
# MAGIC   i.description,
# MAGIC   i.severity,
# MAGIC   i.status,
# MAGIC   i.created_at,
# MAGIC   i.resolved_at,
# MAGIC   i.mttr_minutes,
# MAGIC   i.root_service,
# MAGIC   i.impacted_services,
# MAGIC   i.blast_radius,
# MAGIC   i.domain,
# MAGIC   i.failure_pattern_id,
# MAGIC   i.failure_pattern_name,
# MAGIC   i.environment,
# MAGIC   i.region,
# MAGIC   i.revenue_impact_usd,
# MAGIC   i.sla_breached,
# MAGIC   -- Business context fields
# MAGIC   i.business_unit,
# MAGIC   i.affected_user_count,
# MAGIC   -- Backward-compatible alias used by legacy API/UI queries
# MAGIC   i.affected_user_count as user_impact_count,
# MAGIC   i.affected_roles,
# MAGIC   i.productivity_loss_hours,
# MAGIC   i.productivity_loss_usd,
# MAGIC   i.shipments_delayed,
# MAGIC   i.servicenow_ticket_count,
# MAGIC   i.servicenow_duplicate_tickets,
# MAGIC   i.downstream_impact_narrative,
# MAGIC   i.root_cause_explanation,
# MAGIC   i.revenue_model,
# MAGIC   -- Enrichments
# MAGIC   CASE
# MAGIC     WHEN i.severity = 'P1' THEN 'critical'
# MAGIC     WHEN i.severity = 'P2' THEN 'high'
# MAGIC     WHEN i.severity = 'P3' THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as severity_level,
# MAGIC   DATE(i.created_at) as incident_date,
# MAGIC   HOUR(i.created_at) as incident_hour,
# MAGIC   DAYOFWEEK(i.created_at) as incident_day_of_week,
# MAGIC   WEEKOFYEAR(i.created_at) as incident_week,
# MAGIC   (
# MAGIC     SELECT collect_list(DISTINCT a.alert_name)
# MAGIC     FROM bldemos.eo_analytics.bronze_alerts a
# MAGIC     WHERE a.incident_id = i.incident_id
# MAGIC   ) as correlated_alert_types,
# MAGIC   (
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM bldemos.eo_analytics.bronze_alerts a
# MAGIC     WHERE a.incident_id = i.incident_id
# MAGIC   ) as correlated_alert_count,
# MAGIC   (
# MAGIC     SELECT collect_list(struct(c.change_id, c.change_type, c.service, c.executed_at))
# MAGIC     FROM bldemos.eo_analytics.bronze_topology_changes c
# MAGIC     WHERE c.executed_at BETWEEN i.created_at - INTERVAL 2 HOURS AND i.created_at
# MAGIC       AND (c.service = i.root_service OR array_contains(i.impacted_services, c.service))
# MAGIC   ) as preceding_changes,
# MAGIC   ROUND(
# MAGIC     (CASE WHEN i.severity = 'P1' THEN 100 WHEN i.severity = 'P2' THEN 60 WHEN i.severity = 'P3' THEN 30 ELSE 10 END)
# MAGIC     * (1 + LOG2(GREATEST(i.blast_radius, 1)))
# MAGIC     * (1 + LEAST(i.revenue_impact_usd / 100000, 5))
# MAGIC     , 2
# MAGIC   ) as impact_score,
# MAGIC   current_timestamp() as enriched_at
# MAGIC FROM bldemos.eo_analytics.bronze_incidents i;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_alerts: Enriched alerts with breach analysis
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_alerts AS
# MAGIC SELECT
# MAGIC   a.alert_id,
# MAGIC   a.incident_id,
# MAGIC   a.service,
# MAGIC   a.alert_name,
# MAGIC   a.severity,
# MAGIC   a.fired_at,
# MAGIC   a.resolved_at,
# MAGIC   a.threshold_value,
# MAGIC   a.actual_value,
# MAGIC   a.domain,
# MAGIC   a.environment,
# MAGIC   CASE WHEN a.incident_id IS NOT NULL THEN true ELSE false END as is_incident_correlated,
# MAGIC   TIMESTAMPDIFF(MINUTE, a.fired_at, a.resolved_at) as duration_minutes,
# MAGIC   DATE(a.fired_at) as alert_date,
# MAGIC   HOUR(a.fired_at) as alert_hour,
# MAGIC   DAYOFWEEK(a.fired_at) as alert_day_of_week,
# MAGIC   ROUND(
# MAGIC     CASE WHEN a.threshold_value > 0 THEN (a.actual_value - a.threshold_value) / a.threshold_value * 100
# MAGIC     ELSE 0 END
# MAGIC   , 2) as breach_magnitude_pct,
# MAGIC   CASE
# MAGIC     WHEN a.incident_id IS NOT NULL THEN (
# MAGIC       SELECT CASE WHEN a.fired_at < i.created_at THEN true ELSE false END
# MAGIC       FROM bldemos.eo_analytics.bronze_incidents i
# MAGIC       WHERE i.incident_id = a.incident_id
# MAGIC       LIMIT 1
# MAGIC     )
# MAGIC     ELSE false
# MAGIC   END as is_pre_incident_signal,
# MAGIC   current_timestamp() as enriched_at
# MAGIC FROM bldemos.eo_analytics.bronze_alerts a;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_changes: Enriched changes with risk scoring
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_changes AS
# MAGIC SELECT
# MAGIC   c.change_id,
# MAGIC   c.service,
# MAGIC   c.change_type,
# MAGIC   c.description,
# MAGIC   c.executed_at,
# MAGIC   c.executed_by,
# MAGIC   c.risk_level,
# MAGIC   c.rollback_available,
# MAGIC   c.domain,
# MAGIC   c.environment,
# MAGIC   c.region,
# MAGIC   DATE(c.executed_at) as change_date,
# MAGIC   HOUR(c.executed_at) as change_hour,
# MAGIC   DAYOFWEEK(c.executed_at) as change_day_of_week,
# MAGIC   CASE
# MAGIC     WHEN c.risk_level = 'high' THEN 3.0
# MAGIC     WHEN c.risk_level = 'medium' THEN 2.0
# MAGIC     ELSE 1.0
# MAGIC   END
# MAGIC   * CASE WHEN c.rollback_available THEN 1.0 ELSE 1.5 END
# MAGIC   * CASE
# MAGIC       WHEN c.change_type IN ('database_migration', 'network_route_change', 'firewall_rule_update', 'vpc_peering_update', 'security_group_change') THEN 2.0
# MAGIC       WHEN c.change_type IN ('deployment', 'dependency_upgrade', 'terraform_apply') THEN 1.5
# MAGIC       ELSE 1.0
# MAGIC     END as risk_score,
# MAGIC   (
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM bldemos.eo_analytics.bronze_incidents i
# MAGIC     WHERE i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 4 HOURS
# MAGIC       AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
# MAGIC   ) as incidents_within_4h,
# MAGIC   (
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM bldemos.eo_analytics.bronze_incidents i
# MAGIC     WHERE i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 24 HOURS
# MAGIC       AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
# MAGIC   ) as incidents_within_24h,
# MAGIC   current_timestamp() as enriched_at
# MAGIC FROM bldemos.eo_analytics.bronze_topology_changes c;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_service_health: Daily composite health scores
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_service_health AS
# MAGIC WITH daily_metrics AS (
# MAGIC   SELECT
# MAGIC     service_name,
# MAGIC     DATE(event_timestamp) as metric_date,
# MAGIC     metric_name,
# MAGIC     AVG(metric_value) as avg_value,
# MAGIC     MAX(metric_value) as max_value,
# MAGIC     MIN(metric_value) as min_value
# MAGIC   FROM bldemos.eo_analytics.bronze_metrics
# MAGIC   WHERE service_name IS NOT NULL
# MAGIC   GROUP BY service_name, DATE(event_timestamp), metric_name
# MAGIC ),
# MAGIC daily_incidents AS (
# MAGIC   SELECT
# MAGIC     root_service as service_name,
# MAGIC     DATE(created_at) as incident_date,
# MAGIC     COUNT(*) as incident_count,
# MAGIC     SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
# MAGIC     SUM(blast_radius) as total_blast_radius,
# MAGIC     AVG(mttr_minutes) as avg_mttr
# MAGIC   FROM bldemos.eo_analytics.bronze_incidents
# MAGIC   GROUP BY root_service, DATE(created_at)
# MAGIC ),
# MAGIC daily_errors AS (
# MAGIC   SELECT
# MAGIC     service_name,
# MAGIC     DATE(event_timestamp) as log_date,
# MAGIC     COUNT(*) as total_logs,
# MAGIC     SUM(CASE WHEN severity_text IN ('ERROR', 'FATAL') THEN 1 ELSE 0 END) as error_count
# MAGIC   FROM bldemos.eo_analytics.bronze_logs
# MAGIC   WHERE service_name IS NOT NULL
# MAGIC   GROUP BY service_name, DATE(event_timestamp)
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(m_cpu.service_name, i.service_name, e.service_name) as service_name,
# MAGIC   COALESCE(m_cpu.metric_date, i.incident_date, e.log_date) as health_date,
# MAGIC   m_cpu.avg_value as avg_cpu_pct,
# MAGIC   m_cpu.max_value as max_cpu_pct,
# MAGIC   m_mem.avg_value as avg_memory_pct,
# MAGIC   m_mem.max_value as max_memory_pct,
# MAGIC   m_lat.avg_value as avg_latency_ms,
# MAGIC   m_lat.max_value as max_latency_ms,
# MAGIC   COALESCE(i.incident_count, 0) as incident_count,
# MAGIC   COALESCE(i.p1_count, 0) as p1_incident_count,
# MAGIC   COALESCE(i.total_blast_radius, 0) as total_blast_radius,
# MAGIC   COALESCE(i.avg_mttr, 0) as avg_mttr_minutes,
# MAGIC   COALESCE(e.error_count, 0) as error_log_count,
# MAGIC   COALESCE(e.total_logs, 0) as total_log_count,
# MAGIC   CASE WHEN COALESCE(e.total_logs, 0) > 0
# MAGIC     THEN ROUND(COALESCE(e.error_count, 0) * 100.0 / e.total_logs, 2)
# MAGIC     ELSE 0
# MAGIC   END as error_rate_pct,
# MAGIC   ROUND(
# MAGIC     100
# MAGIC     - LEAST(COALESCE(m_cpu.max_value, 0), 100) * 0.15
# MAGIC     - LEAST(COALESCE(m_mem.max_value, 0), 100) * 0.10
# MAGIC     - COALESCE(i.incident_count, 0) * 15
# MAGIC     - COALESCE(i.p1_count, 0) * 25
# MAGIC     - LEAST(COALESCE(e.error_count, 0) * 0.1, 20)
# MAGIC   , 2) as health_score,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM daily_metrics m_cpu
# MAGIC LEFT JOIN daily_metrics m_mem
# MAGIC   ON m_cpu.service_name = m_mem.service_name AND m_cpu.metric_date = m_mem.metric_date
# MAGIC   AND m_mem.metric_name = 'system.memory.utilization'
# MAGIC LEFT JOIN daily_metrics m_lat
# MAGIC   ON m_cpu.service_name = m_lat.service_name AND m_cpu.metric_date = m_lat.metric_date
# MAGIC   AND m_lat.metric_name = 'http.server.request.duration'
# MAGIC LEFT JOIN daily_incidents i
# MAGIC   ON m_cpu.service_name = i.service_name AND m_cpu.metric_date = i.incident_date
# MAGIC LEFT JOIN daily_errors e
# MAGIC   ON m_cpu.service_name = e.service_name AND m_cpu.metric_date = e.log_date
# MAGIC WHERE m_cpu.metric_name = 'system.cpu.utilization';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_business_impact: Business impact classification with revenue model
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_business_impact AS
# MAGIC SELECT
# MAGIC   i.incident_id,
# MAGIC   i.title,
# MAGIC   i.severity,
# MAGIC   i.root_service,
# MAGIC   i.domain,
# MAGIC   i.business_unit,
# MAGIC   i.created_at,
# MAGIC   i.resolved_at,
# MAGIC   i.mttr_minutes,
# MAGIC   i.revenue_impact_usd,
# MAGIC   i.revenue_model,
# MAGIC   i.affected_user_count,
# MAGIC   i.productivity_loss_usd,
# MAGIC   i.shipments_delayed,
# MAGIC   i.sla_breached,
# MAGIC   i.blast_radius,
# MAGIC   i.failure_pattern_id,
# MAGIC   i.failure_pattern_name,
# MAGIC   i.downstream_impact_narrative,
# MAGIC   i.root_cause_explanation,
# MAGIC   CASE
# MAGIC     WHEN i.revenue_impact_usd > 1000000 THEN 'critical'
# MAGIC     WHEN i.revenue_impact_usd > 100000 THEN 'high'
# MAGIC     WHEN i.revenue_impact_usd > 10000 THEN 'moderate'
# MAGIC     ELSE 'low'
# MAGIC   END as revenue_impact_level,
# MAGIC   CASE
# MAGIC     WHEN i.severity = 'P1' THEN i.mttr_minutes * i.blast_radius
# MAGIC     WHEN i.severity = 'P2' THEN i.mttr_minutes * i.blast_radius * 0.5
# MAGIC     ELSE i.mttr_minutes * i.blast_radius * 0.25
# MAGIC   END as weighted_downtime_minutes,
# MAGIC   DATE(i.created_at) as impact_date,
# MAGIC   WEEKOFYEAR(i.created_at) as impact_week,
# MAGIC   MONTH(i.created_at) as impact_month,
# MAGIC   YEAR(i.created_at) as impact_year,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM bldemos.eo_analytics.bronze_incidents i;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- silver_servicenow_correlation: ServiceNow ticket dedup analysis (NEW)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.silver_servicenow_correlation AS
# MAGIC SELECT
# MAGIC   i.incident_id,
# MAGIC   i.business_unit,
# MAGIC   i.failure_pattern_name,
# MAGIC   i.servicenow_ticket_count,
# MAGIC   i.servicenow_duplicate_tickets,
# MAGIC   ROUND(i.servicenow_duplicate_tickets * 100.0 / NULLIF(i.servicenow_ticket_count, 0), 1) as duplicate_pct,
# MAGIC   i.affected_user_count,
# MAGIC   i.affected_roles,
# MAGIC   i.root_cause_explanation,
# MAGIC   i.downstream_impact_narrative,
# MAGIC   i.revenue_impact_usd,
# MAGIC   i.productivity_loss_usd,
# MAGIC   i.shipments_delayed,
# MAGIC   i.severity,
# MAGIC   i.root_service,
# MAGIC   i.domain,
# MAGIC   i.mttr_minutes,
# MAGIC   i.created_at
# MAGIC FROM bldemos.eo_analytics.bronze_incidents i
# MAGIC WHERE i.servicenow_ticket_count > 0;
