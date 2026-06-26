"""
00_create_schema_and_volume.py
Creates the Unity Catalog schema and volume for raw telemetry landing.
Run with: python data_setup/00_create_schema_and_volume.py
"""
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME = "raw_landing"

def main():
    w = WorkspaceClient(profile=PROFILE)

    # Create schema if not exists
    print(f"Creating schema {CATALOG}.{SCHEMA} ...")
    try:
        w.schemas.create(name=SCHEMA, catalog_name=CATALOG, comment="Enterprise RCA Intelligence - root cause analysis telemetry")
        print(f"  Schema {CATALOG}.{SCHEMA} created.")
    except Exception as e:
        if "SCHEMA_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"  Schema {CATALOG}.{SCHEMA} already exists.")
        else:
            raise

    # Create volume if not exists
    print(f"Creating volume {CATALOG}.{SCHEMA}.{VOLUME} ...")
    try:
        w.volumes.create(
            catalog_name=CATALOG,
            schema_name=SCHEMA,
            name=VOLUME,
            volume_type=VolumeType.MANAGED,
            comment="Raw OpenTelemetry data landing zone"
        )
        print(f"  Volume {CATALOG}.{SCHEMA}.{VOLUME} created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Volume {CATALOG}.{SCHEMA}.{VOLUME} already exists.")
        else:
            raise

    # Create subdirectories in the volume
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
    subdirs = ["metrics", "logs", "traces", "events", "network_flows"]
    for subdir in subdirs:
        full_path = f"{volume_path}/{subdir}"
        print(f"Creating directory {full_path} ...")
        try:
            w.files.create_directory(full_path)
            print(f"  Directory {full_path} created.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  Directory {full_path} already exists.")
            else:
                print(f"  Warning: {e}")

    print("\nDone. Schema, volume, and subdirectories are ready.")
    print(f"Volume path: {volume_path}")

if __name__ == "__main__":
    main()
