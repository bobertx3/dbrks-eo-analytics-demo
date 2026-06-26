"""
06_create_genie_space.py
Fully provisions a Databricks Genie Space for natural language Q&A over the
Enterprise RCA Intelligence gold/silver tables — including attaching the tables
and seeding sample questions (no manual UI step required).

Idempotent: if GENIE_SPACE_ID is set in the environment, the existing space is
updated in place; otherwise a new space is created and its ID is printed.
"""
import os
import json
import uuid
from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "d119a93099e7209f")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "").strip()

SPACE_TITLE = "Enterprise RCA Intelligence -- Business Impact Q&A"
SPACE_DESCRIPTION = (
    "Ask natural language questions about incidents, root causes, and business impact "
    "across Dbrks business domains including Supply Chain, Digital Surgery, "
    "Clinical Trials, and Commercial Pharma."
)

# Tables exposed to Genie (queried via the SQL warehouse against UC Delta tables).
TABLES = [
    "gold_root_cause_patterns",
    "gold_service_risk_ranking",
    "gold_business_impact_summary",
    "gold_domain_impact_summary",
    "silver_incidents",
    "silver_servicenow_correlation",
]

SAMPLE_QUESTIONS = [
    "What are the top 5 incidents by business impact?",
    "Which services have the highest risk ranking?",
    "What is the most common root cause pattern across incidents?",
    "Which business domain has the largest revenue impact from incidents?",
    "How many ServiceNow tickets were duplicates last month?",
]


def build_serialized_space() -> str:
    return json.dumps({
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS
            ]
        },
        "data_sources": {
            # API requires tables sorted by identifier.
            "tables": [
                {"identifier": ident}
                for ident in sorted(f"{CATALOG}.{SCHEMA}.{t}" for t in TABLES)
            ]
        },
    })


def main():
    w = WorkspaceClient(profile=PROFILE)
    serialized = build_serialized_space()
    host = w.config.host

    if GENIE_SPACE_ID:
        print(f"Updating existing Genie Space {GENIE_SPACE_ID} ...")
        space = w.genie.update_space(
            space_id=GENIE_SPACE_ID,
            serialized_space=serialized,
            title=SPACE_TITLE,
            description=SPACE_DESCRIPTION,
            warehouse_id=WAREHOUSE_ID,
        )
        space_id = GENIE_SPACE_ID
        action = "updated"
    else:
        print(f"Creating Genie Space (warehouse {WAREHOUSE_ID}) ...")
        space = w.genie.create_space(
            warehouse_id=WAREHOUSE_ID,
            serialized_space=serialized,
            title=SPACE_TITLE,
            description=SPACE_DESCRIPTION,
        )
        space_id = space.space_id
        action = "created"

    print(f"\n  Genie Space {action} successfully with {len(TABLES)} tables!")
    print(f"  Space ID: {space_id}")
    print(f"  URL: {host}/explore/genie/{space_id}")
    print(f"\n  GENIE_SPACE_ID={space_id}")
    print("  Tables attached:")
    for t in TABLES:
        print(f"    - {CATALOG}.{SCHEMA}.{t}")

    return space_id


if __name__ == "__main__":
    main()
