"""
00b_load_static_data.py
Loads repository static_data files into the Unity Catalog raw_landing volume.

Expected source layout at repo root:
  static_data/
    events/
    logs/
    metrics/
    network_flows/
    traces/

Run after:
  python setup_pipeline/00_create_schema_and_volume.py
"""
import os
from pathlib import Path
from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = os.environ.get("CATALOG", "bldemos")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics")
VOLUME = "raw_landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
REQUIRED_SUBDIRS = ("events", "logs", "metrics", "network_flows", "traces")


def get_static_data_dir() -> Path:
    # setup_pipeline/<this_file>.py -> repo root is parent of setup_pipeline
    repo_root = Path(__file__).resolve().parent.parent
    return repo_root / "static_data"


def upload_directory(w: WorkspaceClient, local_dir: Path, volume_dir: str) -> int:
    uploaded = 0
    for root, _, files in os.walk(local_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(local_dir)
        target_dir = volume_dir if str(rel_root) == "." else f"{volume_dir}/{rel_root.as_posix()}"
        w.files.create_directory(target_dir)

        for filename in files:
            local_path = root_path / filename
            remote_path = f"{target_dir}/{filename}"
            with open(local_path, "rb") as f:
                w.files.upload(remote_path, f, overwrite=True)
            uploaded += 1
            print(f"  Uploaded {local_path.name} -> {remote_path}")
    return uploaded


def main():
    static_data_dir = get_static_data_dir()
    if not static_data_dir.exists() or not static_data_dir.is_dir():
        raise FileNotFoundError(
            f"Could not find static_data directory at {static_data_dir}. "
            "Create/populate static_data first."
        )

    missing = [d for d in REQUIRED_SUBDIRS if not (static_data_dir / d).is_dir()]
    if missing:
        raise FileNotFoundError(
            f"Missing required static_data subdirectories: {missing}. "
            f"Expected: {list(REQUIRED_SUBDIRS)}"
        )

    w = WorkspaceClient(profile=PROFILE)
    total_uploaded = 0

    print(f"Loading static data from: {static_data_dir}")
    print(f"Target volume: {VOLUME_PATH}")

    for subdir in REQUIRED_SUBDIRS:
        local_subdir = static_data_dir / subdir
        remote_subdir = f"{VOLUME_PATH}/{subdir}"
        print(f"\nSyncing {subdir} ...")
        w.files.create_directory(remote_subdir)
        uploaded = upload_directory(w, local_subdir, remote_subdir)
        total_uploaded += uploaded
        print(f"  {subdir}: uploaded {uploaded} files")

    print(f"\nDone. Uploaded {total_uploaded} files into {VOLUME_PATH}.")
    print("You can now run 01/02 generators; they should skip because data exists.")


if __name__ == "__main__":
    main()
