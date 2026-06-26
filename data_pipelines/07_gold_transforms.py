# Databricks notebook source
# MAGIC %sql
# MAGIC -- Gold Layer Transforms for Enterprise RCA Intelligence Pipeline
# MAGIC -- Creates 5 analytics-ready gold tables from silver data
# MAGIC -- Run against: bldemos.eo_analytics
# MAGIC --
# MAGIC -- Updated for Dbrks business unit schema with:
# MAGIC --   business_unit, total_affected_users, servicenow_tickets, revenue_model
# MAGIC -- New table: gold_business_impact_summary
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- gold_root_cause_patterns: Recurring failure pattern analysis
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.gold_root_cause_patterns AS
# MAGIC WITH impacted_services_agg AS (
# MAGIC   SELECT
# MAGIC     failure_pattern_id,
# MAGIC     collect_set(svc) as all_impacted_services
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   LATERAL VIEW explode(impacted_services) explode_svc AS svc
# MAGIC   WHERE failure_pattern_id IS NOT NULL
# MAGIC   GROUP BY failure_pattern_id
# MAGIC ),
# MAGIC pattern_stats AS (
# MAGIC   SELECT
# MAGIC     failure_pattern_id,
# MAGIC     failure_pattern_name,
# MAGIC     root_service,
# MAGIC     domain,
# MAGIC     first(business_unit) as business_unit,
# MAGIC     COUNT(*) as occurrence_count,
# MAGIC     AVG(mttr_minutes) as avg_mttr_minutes,
# MAGIC     MAX(mttr_minutes) as max_mttr_minutes,
# MAGIC     MIN(mttr_minutes) as min_mttr_minutes,
# MAGIC     PERCENTILE(mttr_minutes, 0.5) as p50_mttr_minutes,
# MAGIC     PERCENTILE(mttr_minutes, 0.95) as p95_mttr_minutes,
# MAGIC     AVG(blast_radius) as avg_blast_radius,
# MAGIC     MAX(blast_radius) as max_blast_radius,
# MAGIC     SUM(revenue_impact_usd) as total_revenue_impact,
# MAGIC     AVG(revenue_impact_usd) as avg_revenue_impact,
# MAGIC     SUM(affected_user_count) as total_affected_users,
# MAGIC     AVG(affected_user_count) as avg_affected_users,
# MAGIC     SUM(servicenow_ticket_count) as total_servicenow_tickets,
# MAGIC     SUM(servicenow_duplicate_tickets) as total_duplicate_tickets,
# MAGIC     SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
# MAGIC     SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
# MAGIC     SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
# MAGIC     SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breach_count,
# MAGIC     MIN(created_at) as first_occurrence,
# MAGIC     MAX(created_at) as last_occurrence,
# MAGIC     first(revenue_model) as revenue_model,
# MAGIC     first(root_cause_explanation) as root_cause_explanation,
# MAGIC     collect_set(root_service) as affected_root_services
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   WHERE failure_pattern_id IS NOT NULL
# MAGIC   GROUP BY failure_pattern_id, failure_pattern_name, root_service, domain
# MAGIC ),
# MAGIC weekly_trend AS (
# MAGIC   SELECT
# MAGIC     failure_pattern_id,
# MAGIC     WEEKOFYEAR(created_at) as week_num,
# MAGIC     YEAR(created_at) as year_num,
# MAGIC     COUNT(*) as weekly_count
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   WHERE failure_pattern_id IS NOT NULL
# MAGIC   GROUP BY failure_pattern_id, WEEKOFYEAR(created_at), YEAR(created_at)
# MAGIC ),
# MAGIC trend_summary AS (
# MAGIC   SELECT
# MAGIC     failure_pattern_id,
# MAGIC     AVG(CASE WHEN (year_num * 52 + week_num) >= ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 4)
# MAGIC       THEN weekly_count END) as recent_avg,
# MAGIC     AVG(CASE WHEN (year_num * 52 + week_num) BETWEEN
# MAGIC       ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 8)
# MAGIC       AND ((SELECT MAX(year_num * 52 + week_num) FROM weekly_trend) - 4)
# MAGIC       THEN weekly_count END) as previous_avg
# MAGIC   FROM weekly_trend
# MAGIC   GROUP BY failure_pattern_id
# MAGIC )
# MAGIC SELECT
# MAGIC   ps.*,
# MAGIC   isa.all_impacted_services,
# MAGIC   CASE
# MAGIC     WHEN ts.recent_avg > ts.previous_avg * 1.2 THEN 'worsening'
# MAGIC     WHEN ts.recent_avg < ts.previous_avg * 0.8 THEN 'improving'
# MAGIC     ELSE 'stable'
# MAGIC   END as trend_direction,
# MAGIC   ROUND(ts.recent_avg, 2) as recent_weekly_avg,
# MAGIC   ROUND(ts.previous_avg, 2) as previous_weekly_avg,
# MAGIC   ROUND(
# MAGIC     ps.occurrence_count * 2.0
# MAGIC     + ps.total_revenue_impact / 10000.0
# MAGIC     + ps.total_affected_users / 10.0
# MAGIC     + ps.p1_count * 20.0
# MAGIC     + ps.sla_breach_count * 15.0
# MAGIC     + ps.avg_blast_radius * 5.0
# MAGIC     + CASE WHEN ts.recent_avg > ts.previous_avg * 1.2 THEN 50 ELSE 0 END
# MAGIC   , 2) as priority_score,
# MAGIC   CASE WHEN ps.occurrence_count > 1
# MAGIC     THEN ROUND(DATEDIFF(ps.last_occurrence, ps.first_occurrence) / (ps.occurrence_count - 1.0), 1)
# MAGIC     ELSE NULL
# MAGIC   END as avg_days_between_occurrences,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM pattern_stats ps
# MAGIC LEFT JOIN impacted_services_agg isa ON ps.failure_pattern_id = isa.failure_pattern_id
# MAGIC LEFT JOIN trend_summary ts ON ps.failure_pattern_id = ts.failure_pattern_id
# MAGIC ORDER BY priority_score DESC;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- gold_service_risk_ranking: Composite risk scoring per service
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.gold_service_risk_ranking AS
# MAGIC WITH incident_stats AS (
# MAGIC   SELECT
# MAGIC     root_service as service_name,
# MAGIC     first(business_unit) as business_unit,
# MAGIC     COUNT(*) as incident_count,
# MAGIC     SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
# MAGIC     SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
# MAGIC     SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
# MAGIC     AVG(mttr_minutes) as avg_mttr,
# MAGIC     SUM(blast_radius) as total_blast_radius,
# MAGIC     AVG(blast_radius) as avg_blast_radius,
# MAGIC     SUM(revenue_impact_usd) as total_revenue_impact,
# MAGIC     SUM(affected_user_count) as total_affected_users,
# MAGIC     SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breaches,
# MAGIC     COUNT(DISTINCT failure_pattern_id) as unique_failure_patterns,
# MAGIC     AVG(impact_score) as avg_impact_score
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   GROUP BY root_service
# MAGIC ),
# MAGIC impacted_stats AS (
# MAGIC   SELECT
# MAGIC     svc as service_name,
# MAGIC     COUNT(*) as times_impacted
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   LATERAL VIEW explode(impacted_services) t AS svc
# MAGIC   GROUP BY svc
# MAGIC ),
# MAGIC health_stats AS (
# MAGIC   SELECT
# MAGIC     service_name,
# MAGIC     AVG(health_score) as avg_health_score,
# MAGIC     MIN(health_score) as min_health_score,
# MAGIC     AVG(error_rate_pct) as avg_error_rate,
# MAGIC     AVG(avg_cpu_pct) as avg_cpu
# MAGIC   FROM bldemos.eo_analytics.silver_service_health
# MAGIC   GROUP BY service_name
# MAGIC ),
# MAGIC change_stats AS (
# MAGIC   SELECT
# MAGIC     service,
# MAGIC     COUNT(*) as total_changes,
# MAGIC     SUM(incidents_within_4h) as changes_followed_by_incidents
# MAGIC   FROM bldemos.eo_analytics.silver_changes
# MAGIC   GROUP BY service
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(ist.service_name, imp.service_name, hs.service_name) as service_name,
# MAGIC   COALESCE(ist.business_unit, 'shared-infrastructure') as business_unit,
# MAGIC   COALESCE(ist.incident_count, 0) as incident_count_as_root,
# MAGIC   COALESCE(imp.times_impacted, 0) as times_impacted_by_others,
# MAGIC   COALESCE(ist.p1_count, 0) as p1_count,
# MAGIC   COALESCE(ist.p2_count, 0) as p2_count,
# MAGIC   COALESCE(ist.p3_count, 0) as p3_count,
# MAGIC   COALESCE(ist.avg_mttr, 0) as avg_mttr_minutes,
# MAGIC   COALESCE(ist.total_blast_radius, 0) as total_blast_radius,
# MAGIC   COALESCE(ist.avg_blast_radius, 0) as avg_blast_radius,
# MAGIC   COALESCE(ist.total_revenue_impact, 0) as total_revenue_impact,
# MAGIC   COALESCE(ist.total_affected_users, 0) as total_affected_users,
# MAGIC   COALESCE(ist.sla_breaches, 0) as sla_breaches,
# MAGIC   COALESCE(ist.unique_failure_patterns, 0) as unique_failure_patterns,
# MAGIC   COALESCE(hs.avg_health_score, 100) as avg_health_score,
# MAGIC   COALESCE(hs.min_health_score, 100) as min_health_score,
# MAGIC   COALESCE(hs.avg_error_rate, 0) as avg_error_rate,
# MAGIC   COALESCE(hs.avg_cpu, 0) as avg_cpu_utilization,
# MAGIC   COALESCE(cs.total_changes, 0) as total_changes,
# MAGIC   COALESCE(cs.changes_followed_by_incidents, 0) as risky_changes,
# MAGIC   ROUND(
# MAGIC     COALESCE(ist.incident_count, 0) * 10.0
# MAGIC     + COALESCE(ist.p1_count, 0) * 30.0
# MAGIC     + COALESCE(ist.sla_breaches, 0) * 20.0
# MAGIC     + COALESCE(ist.total_revenue_impact, 0) / 10000.0
# MAGIC     + COALESCE(ist.total_affected_users, 0) / 5.0
# MAGIC     + COALESCE(ist.avg_blast_radius, 0) * 5.0
# MAGIC     + COALESCE(imp.times_impacted, 0) * 2.0
# MAGIC     + (100 - COALESCE(hs.avg_health_score, 100)) * 0.5
# MAGIC     + COALESCE(cs.changes_followed_by_incidents, 0) * 8.0
# MAGIC   , 2) as risk_score,
# MAGIC   ROW_NUMBER() OVER (ORDER BY
# MAGIC     COALESCE(ist.incident_count, 0) * 10.0
# MAGIC     + COALESCE(ist.p1_count, 0) * 30.0
# MAGIC     + COALESCE(ist.total_revenue_impact, 0) / 10000.0
# MAGIC     + COALESCE(ist.total_affected_users, 0) / 5.0
# MAGIC     DESC
# MAGIC   ) as risk_rank,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM incident_stats ist
# MAGIC FULL OUTER JOIN impacted_stats imp ON ist.service_name = imp.service_name
# MAGIC FULL OUTER JOIN health_stats hs ON COALESCE(ist.service_name, imp.service_name) = hs.service_name
# MAGIC LEFT JOIN change_stats cs ON COALESCE(ist.service_name, imp.service_name) = cs.service
# MAGIC ORDER BY risk_score DESC;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- gold_change_incident_correlation: Change-incident causal analysis
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.gold_change_incident_correlation AS
# MAGIC WITH change_incident_pairs AS (
# MAGIC   SELECT
# MAGIC     c.change_id,
# MAGIC     c.service as change_service,
# MAGIC     c.change_type,
# MAGIC     c.risk_level,
# MAGIC     c.risk_score,
# MAGIC     c.executed_at as change_time,
# MAGIC     c.executed_by,
# MAGIC     i.incident_id,
# MAGIC     i.severity as incident_severity,
# MAGIC     i.title as incident_title,
# MAGIC     i.root_service as incident_root_service,
# MAGIC     i.business_unit,
# MAGIC     i.created_at as incident_time,
# MAGIC     i.mttr_minutes,
# MAGIC     i.blast_radius,
# MAGIC     i.revenue_impact_usd,
# MAGIC     i.failure_pattern_id,
# MAGIC     TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) as minutes_between,
# MAGIC     CASE
# MAGIC       WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 30 THEN 'immediate'
# MAGIC       WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 120 THEN 'short_delay'
# MAGIC       WHEN TIMESTAMPDIFF(MINUTE, c.executed_at, i.created_at) <= 480 THEN 'delayed'
# MAGIC       ELSE 'long_delay'
# MAGIC     END as correlation_window
# MAGIC   FROM bldemos.eo_analytics.silver_changes c
# MAGIC   JOIN bldemos.eo_analytics.silver_incidents i
# MAGIC     ON i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 24 HOURS
# MAGIC     AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
# MAGIC ),
# MAGIC change_type_stats AS (
# MAGIC   SELECT
# MAGIC     change_type,
# MAGIC     COUNT(DISTINCT change_id) as total_changes,
# MAGIC     COUNT(DISTINCT incident_id) as incidents_caused,
# MAGIC     ROUND(COUNT(DISTINCT incident_id) * 100.0 / NULLIF(COUNT(DISTINCT change_id), 0), 2) as incident_rate_pct,
# MAGIC     AVG(minutes_between) as avg_time_to_incident,
# MAGIC     SUM(revenue_impact_usd) as total_impact_usd,
# MAGIC     SUM(blast_radius) as total_blast_radius
# MAGIC   FROM change_incident_pairs
# MAGIC   GROUP BY change_type
# MAGIC )
# MAGIC SELECT
# MAGIC   cip.*,
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN cip.correlation_window = 'immediate' THEN 0.9
# MAGIC       WHEN cip.correlation_window = 'short_delay' THEN 0.7
# MAGIC       WHEN cip.correlation_window = 'delayed' THEN 0.4
# MAGIC       ELSE 0.2
# MAGIC     END
# MAGIC     * CASE WHEN cip.change_service = cip.incident_root_service THEN 1.5 ELSE 1.0 END
# MAGIC     * cip.risk_score / 3.0
# MAGIC   , 3) as correlation_strength,
# MAGIC   cts.incident_rate_pct as change_type_incident_rate,
# MAGIC   cts.total_changes as change_type_total_count,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM change_incident_pairs cip
# MAGIC LEFT JOIN change_type_stats cts ON cip.change_type = cts.change_type
# MAGIC ORDER BY correlation_strength DESC;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- gold_domain_impact_summary: Daily domain-level impact aggregation
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.gold_domain_impact_summary AS
# MAGIC WITH domain_incidents AS (
# MAGIC   SELECT
# MAGIC     domain,
# MAGIC     DATE(created_at) as incident_date,
# MAGIC     MONTH(created_at) as incident_month,
# MAGIC     YEAR(created_at) as incident_year,
# MAGIC     COUNT(*) as incident_count,
# MAGIC     SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
# MAGIC     SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
# MAGIC     SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
# MAGIC     AVG(mttr_minutes) as avg_mttr,
# MAGIC     SUM(blast_radius) as total_blast_radius,
# MAGIC     SUM(revenue_impact_usd) as total_revenue_impact,
# MAGIC     SUM(affected_user_count) as total_affected_users,
# MAGIC     SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as sla_breaches,
# MAGIC     collect_set(root_service) as affected_services,
# MAGIC     collect_set(failure_pattern_id) as failure_patterns
# MAGIC   FROM bldemos.eo_analytics.silver_incidents
# MAGIC   GROUP BY domain, DATE(created_at), MONTH(created_at), YEAR(created_at)
# MAGIC ),
# MAGIC domain_alerts AS (
# MAGIC   SELECT
# MAGIC     domain,
# MAGIC     DATE(fired_at) as alert_date,
# MAGIC     COUNT(*) as alert_count,
# MAGIC     SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_alert_count,
# MAGIC     SUM(CASE WHEN is_pre_incident_signal THEN 1 ELSE 0 END) as pre_incident_signals
# MAGIC   FROM bldemos.eo_analytics.silver_alerts
# MAGIC   GROUP BY domain, DATE(fired_at)
# MAGIC ),
# MAGIC domain_changes AS (
# MAGIC   SELECT
# MAGIC     domain,
# MAGIC     DATE(executed_at) as change_date,
# MAGIC     COUNT(*) as change_count,
# MAGIC     SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_changes,
# MAGIC     SUM(incidents_within_4h) as changes_causing_incidents
# MAGIC   FROM bldemos.eo_analytics.silver_changes
# MAGIC   GROUP BY domain, DATE(executed_at)
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(di.domain, da.domain, dc.domain) as domain,
# MAGIC   COALESCE(di.incident_date, da.alert_date, dc.change_date) as summary_date,
# MAGIC   COALESCE(di.incident_month, MONTH(da.alert_date), MONTH(dc.change_date)) as summary_month,
# MAGIC   COALESCE(di.incident_year, YEAR(da.alert_date), YEAR(dc.change_date)) as summary_year,
# MAGIC   COALESCE(di.incident_count, 0) as incident_count,
# MAGIC   COALESCE(di.p1_count, 0) as p1_count,
# MAGIC   COALESCE(di.p2_count, 0) as p2_count,
# MAGIC   COALESCE(di.p3_count, 0) as p3_count,
# MAGIC   COALESCE(di.avg_mttr, 0) as avg_mttr_minutes,
# MAGIC   COALESCE(di.total_blast_radius, 0) as total_blast_radius,
# MAGIC   COALESCE(di.total_revenue_impact, 0) as total_revenue_impact,
# MAGIC   COALESCE(di.total_affected_users, 0) as total_affected_users,
# MAGIC   COALESCE(di.sla_breaches, 0) as sla_breaches,
# MAGIC   COALESCE(da.alert_count, 0) as alert_count,
# MAGIC   COALESCE(da.critical_alert_count, 0) as critical_alert_count,
# MAGIC   COALESCE(da.pre_incident_signals, 0) as pre_incident_signals,
# MAGIC   COALESCE(dc.change_count, 0) as change_count,
# MAGIC   COALESCE(dc.high_risk_changes, 0) as high_risk_changes,
# MAGIC   COALESCE(dc.changes_causing_incidents, 0) as changes_causing_incidents,
# MAGIC   ROUND(
# MAGIC     COALESCE(di.incident_count, 0) * 10
# MAGIC     + COALESCE(di.p1_count, 0) * 25
# MAGIC     + COALESCE(di.total_revenue_impact, 0) / 5000
# MAGIC     + COALESCE(di.total_affected_users, 0) / 2
# MAGIC     + COALESCE(di.sla_breaches, 0) * 15
# MAGIC     + COALESCE(dc.changes_causing_incidents, 0) * 8
# MAGIC   , 2) as domain_risk_score,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM domain_incidents di
# MAGIC FULL OUTER JOIN domain_alerts da
# MAGIC   ON di.domain = da.domain AND di.incident_date = da.alert_date
# MAGIC FULL OUTER JOIN domain_changes dc
# MAGIC   ON COALESCE(di.domain, da.domain) = dc.domain
# MAGIC   AND COALESCE(di.incident_date, da.alert_date) = dc.change_date
# MAGIC ORDER BY domain_risk_score DESC;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- gold_business_impact_summary: Per-business-unit aggregation (NEW)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.gold_business_impact_summary AS
# MAGIC SELECT
# MAGIC   i.business_unit,
# MAGIC   COUNT(*) as total_incidents,
# MAGIC   SUM(CASE WHEN i.severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
# MAGIC   SUM(CASE WHEN i.severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
# MAGIC   ROUND(AVG(i.mttr_minutes), 1) as avg_mttr_minutes,
# MAGIC   ROUND(SUM(i.revenue_impact_usd), 2) as total_revenue_impact,
# MAGIC   first(i.revenue_model) as primary_revenue_model,
# MAGIC   SUM(i.affected_user_count) as total_affected_users,
# MAGIC   SUM(i.productivity_loss_usd) as total_productivity_loss,
# MAGIC   SUM(i.shipments_delayed) as total_shipments_delayed,
# MAGIC   SUM(i.servicenow_ticket_count) as total_servicenow_tickets,
# MAGIC   SUM(i.servicenow_duplicate_tickets) as total_duplicate_tickets,
# MAGIC   ROUND(SUM(i.servicenow_duplicate_tickets) * 100.0 / NULLIF(SUM(i.servicenow_ticket_count), 0), 1) as overall_duplicate_pct,
# MAGIC   ROUND(AVG(i.blast_radius), 1) as avg_blast_radius,
# MAGIC   SUM(CASE WHEN i.sla_breached THEN 1 ELSE 0 END) as sla_breaches,
# MAGIC   COUNT(DISTINCT i.failure_pattern_id) as unique_failure_patterns,
# MAGIC   COUNT(DISTINCT i.root_service) as affected_services_count,
# MAGIC   current_timestamp() as computed_at
# MAGIC FROM bldemos.eo_analytics.silver_incidents i
# MAGIC GROUP BY i.business_unit
# MAGIC ORDER BY total_revenue_impact DESC;
