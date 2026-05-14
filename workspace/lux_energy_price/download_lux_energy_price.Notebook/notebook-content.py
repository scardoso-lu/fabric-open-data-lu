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

API_BASE = "https://data.public.lu/api/1/datasets/electricity-in-luxembourg-day-ahead-prices/"
SANDBOX_DIR = "Files/data/sandbox/lux_energy_price"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    from notebookutils import mssparkutils
    IS_FABRIC = True
except ImportError:
    IS_FABRIC = False

import os
import urllib.request
import urllib.error
import json
from pathlib import Path

if IS_FABRIC:
    base_path = f"/lakehouse/default/{SANDBOX_DIR}"
else:
    base_path = SANDBOX_DIR

os.makedirs(base_path, exist_ok=True)
print(f"Sandbox dir: {base_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    page = 1
    page_size = 100
    all_resources = []

    while True:
        url = f"{API_BASE}?page={page}&page_size={page_size}"
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read())

        resources = data.get("resources", [])
        all_resources.extend(resources)

        total = data.get("total", len(resources))
        if len(all_resources) >= total or len(resources) == 0:
            break
        page += 1

    print(f"Total resources in API: {len(all_resources)}")
except Exception as e:
    print(f"API fetch failed: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing = 0
downloaded = 0
failed = 0

for resource in all_resources:
    url = resource.get("url", "")
    if not url.endswith(".xml"):
        continue

    filename = url.split("/")[-1]
    dest = os.path.join(base_path, filename)

    if os.path.exists(dest):
        existing += 1
        continue

    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            content = resp.read()
        with open(dest, "wb") as f:
            f.write(content)
        downloaded += 1
    except Exception as e:
        print(f"  FAIL {filename}: {e}")
        failed += 1

print(f"existing={existing}  downloaded={downloaded}  failed={failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
