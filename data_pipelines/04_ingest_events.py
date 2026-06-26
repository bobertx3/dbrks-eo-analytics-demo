# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Events (Incidents, Alerts, Changes)

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.bronze_incidents AS
# MAGIC SELECT
# MAGIC   incident_id,
# MAGIC   title,
# MAGIC   description,
# MAGIC   severity,
# MAGIC   status,
# MAGIC   CAST(created_at AS TIMESTAMP) as created_at,
# MAGIC   CAST(resolved_at AS TIMESTAMP) as resolved_at,
# MAGIC   CAST(mttr_minutes AS INT) as mttr_minutes,
# MAGIC   root_service,
# MAGIC   impacted_services,
# MAGIC   CAST(blast_radius AS INT) as blast_radius,
# MAGIC   domain,
# MAGIC   failure_pattern_id,
# MAGIC   failure_pattern_name,
# MAGIC   environment,
# MAGIC   region,
# MAGIC   CAST(revenue_impact_usd AS DOUBLE) as revenue_impact_usd,
# MAGIC   CAST(sla_breached AS BOOLEAN) as sla_breached,
# MAGIC   business_unit,
# MAGIC   CAST(affected_user_count AS INT) as affected_user_count,
# MAGIC   CAST(affected_user_count AS INT) as user_impact_count,
# MAGIC   affected_roles,
# MAGIC   CAST(productivity_loss_hours AS DOUBLE) as productivity_loss_hours,
# MAGIC   CAST(productivity_loss_usd AS DOUBLE) as productivity_loss_usd,
# MAGIC   CAST(shipments_delayed AS INT) as shipments_delayed,
# MAGIC   CAST(servicenow_ticket_count AS INT) as servicenow_ticket_count,
# MAGIC   CAST(servicenow_duplicate_tickets AS INT) as servicenow_duplicate_tickets,
# MAGIC   downstream_impact_narrative,
# MAGIC   root_cause_explanation,
# MAGIC   revenue_model,
# MAGIC   current_timestamp() as ingested_at
# MAGIC FROM json.`/Volumes/bldemos/eo_analytics/raw_landing/events/incidents.jsonl`;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.bronze_alerts AS
# MAGIC SELECT
# MAGIC   alert_id,
# MAGIC   incident_id,
# MAGIC   service,
# MAGIC   alert_name,
# MAGIC   severity,
# MAGIC   CAST(fired_at AS TIMESTAMP) as fired_at,
# MAGIC   CAST(resolved_at AS TIMESTAMP) as resolved_at,
# MAGIC   CAST(threshold_value AS DOUBLE) as threshold_value,
# MAGIC   CAST(actual_value AS DOUBLE) as actual_value,
# MAGIC   domain,
# MAGIC   environment,
# MAGIC   current_timestamp() as ingested_at
# MAGIC FROM json.`/Volumes/bldemos/eo_analytics/raw_landing/events/alerts.jsonl`;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bldemos.eo_analytics.bronze_topology_changes AS
# MAGIC SELECT
# MAGIC   change_id,
# MAGIC   service,
# MAGIC   change_type,
# MAGIC   description,
# MAGIC   CAST(executed_at AS TIMESTAMP) as executed_at,
# MAGIC   executed_by,
# MAGIC   risk_level,
# MAGIC   CAST(rollback_available AS BOOLEAN) as rollback_available,
# MAGIC   domain,
# MAGIC   environment,
# MAGIC   region,
# MAGIC   current_timestamp() as ingested_at
# MAGIC FROM json.`/Volumes/bldemos/eo_analytics/raw_landing/events/topology_changes.jsonl`;
