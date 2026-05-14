# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f96c5a4c-7777-4fda-aeb9-eb239ed1731c",
# META       "default_lakehouse_name": "DATALAKE",
# META       "default_lakehouse_workspace_id": "ffb5e061-3824-486b-ab7c-aaef61221403",
# META       "known_lakehouses": [
# META         {
# META           "id": "f96c5a4c-7777-4fda-aeb9-eb239ed1731c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# # Download - Luxembourg House Prices by Commune
#
# Source: https://data.public.lu/en/datasets/prix-annonces-des-logements-par-commune/

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

API_URL = "https://data.public.lu/api/1/datasets/prix-annonces-des-logements-par-commune/"
FABRIC_STAGING_DIR = "Files/data/sandbox/lux_house_prices/raw"
LOCAL_STAGING_DIR = "data/sandbox/lux_house_prices/raw"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import pathlib
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse

try:
    from notebookutils import mssparkutils

    IS_FABRIC = True
except ImportError:
    mssparkutils = None
    IS_FABRIC = False

OUTPUT_DIR = FABRIC_STAGING_DIR if IS_FABRIC else LOCAL_STAGING_DIR


def io_path(path: str) -> str:
    if IS_FABRIC and path.startswith("Files/"):
        return f"/lakehouse/default/{path}"
    return path


def ensure_dir(path: str) -> None:
    if IS_FABRIC:
        mssparkutils.fs.mkdirs(path)
    else:
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def list_existing(path: str) -> set[str]:
    if IS_FABRIC:
        try:
            return {item.name for item in mssparkutils.fs.ls(path)}
        except Exception:
            return set()
    if not os.path.isdir(path):
        return set()
    return set(os.listdir(path))


def filename_from_url(url: str) -> str:
    name = pathlib.PurePosixPath(urlparse(url).path).name
    if not name:
        raise ValueError(f"Could not derive filename from URL: {url}")
    return name


def write_binary(path: str, content: bytes) -> None:
    target = io_path(path)
    pathlib.Path(target).parent.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as fh:
        fh.write(content)


def write_json(path: str, payload: dict) -> None:
    target = io_path(path)
    pathlib.Path(target).parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, sort_keys=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    with urllib.request.urlopen(API_URL, timeout=60) as response:
        dataset = json.loads(response.read().decode("utf-8"))
except Exception as exc:
    print(f"API fetch failed: {exc}")
    raise

resources = []
for resource in dataset.get("resources", []):
    url = resource.get("url") or ""
    fmt = (resource.get("format") or "").lower()
    if resource.get("type") == "main" and fmt in {"xls", "xlsx"} and url:
        resources.append(resource)

print(f"dataset={dataset.get('slug')}")
print(f"last_update={dataset.get('last_update')}")
print(f"excel_resources={len(resources)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    ensure_dir(OUTPUT_DIR)
    existing = list_existing(OUTPUT_DIR)
except Exception as exc:
    print(f"Sandbox init failed: {exc}")
    raise

existing_count = 0
downloaded_count = 0
failed_count = 0

for resource in resources:
    url = resource.get("url") or ""
    filename = filename_from_url(url)
    metadata_filename = f"{filename}.metadata.json"

    if filename in existing:
        existing_count += 1
        if metadata_filename not in existing:
            write_json(
                f"{OUTPUT_DIR}/{metadata_filename}",
                {
                    "dataset": {
                        "id": dataset.get("id"),
                        "slug": dataset.get("slug"),
                        "title": dataset.get("title"),
                        "uri": dataset.get("uri"),
                        "last_update": dataset.get("last_update"),
                        "license": dataset.get("license"),
                    },
                    "resource": resource,
                    "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "source_file": filename,
                },
            )
        continue

    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            content = response.read()
        write_binary(f"{OUTPUT_DIR}/{filename}", content)
        write_json(
            f"{OUTPUT_DIR}/{metadata_filename}",
            {
                "dataset": {
                    "id": dataset.get("id"),
                    "slug": dataset.get("slug"),
                    "title": dataset.get("title"),
                    "uri": dataset.get("uri"),
                    "last_update": dataset.get("last_update"),
                    "license": dataset.get("license"),
                },
                "resource": resource,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_file": filename,
                "content_length": len(content),
            },
        )
        downloaded_count += 1
        print(f"downloaded {filename} ({len(content):,} bytes)")
    except Exception as exc:
        failed_count += 1
        print(f"ERROR {filename}: {exc}")

print(f"existing={existing_count} downloaded={downloaded_count} failed={failed_count}")
print(f"bronze_source_dir={OUTPUT_DIR}")

if failed_count:
    raise RuntimeError(f"Failed to download {failed_count} resource(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
