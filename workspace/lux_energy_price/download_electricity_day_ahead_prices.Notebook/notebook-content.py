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

# # Download — Electricity Day-Ahead Prices (Luxembourg)
# Source: https://data.public.lu/en/datasets/electricity-in-luxembourg-day-ahead-prices/

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import urllib.request

try:
    from notebookutils import mssparkutils
    IS_FABRIC = True
except ImportError:
    IS_FABRIC = False

API_URL = "https://data.public.lu/api/1/datasets/electricity-in-luxembourg-day-ahead-prices/"
SANDBOX_DIR = "Files/data/sandbox/lux_energy_price" if IS_FABRIC else "data/sandbox/lux_energy_price"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    with urllib.request.urlopen(API_URL, timeout=30) as r:
        resources = json.loads(r.read()).get("resources", [])
    xml_resources = [res for res in resources if (res.get("url") or "").endswith(".xml")]
    print(f"Found {len(xml_resources)} XML resources on API")
except Exception as exc:
    print(f"API fetch failed: {exc}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if IS_FABRIC:
        try:
            existing = {f.name for f in mssparkutils.fs.ls(SANDBOX_DIR)}
        except Exception:
            existing = set()
            mssparkutils.fs.mkdirs(SANDBOX_DIR)
    else:
        import os, pathlib
        pathlib.Path(SANDBOX_DIR).mkdir(parents=True, exist_ok=True)
        existing = set(os.listdir(SANDBOX_DIR))
except Exception as exc:
    print(f"Sandbox init failed: {exc}")
    raise

existing_count = downloaded = failed = 0

for res in xml_resources:
    url = res.get("url") or ""
    filename = url.rsplit("/", 1)[-1]
    if not filename:
        continue
    if filename in existing:
        existing_count += 1
        continue
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            content = r.read().decode("utf-8")
        if IS_FABRIC:
            mssparkutils.fs.put(f"{SANDBOX_DIR}/{filename}", content, True)
        else:
            with open(f"{SANDBOX_DIR}/{filename}", "w", encoding="utf-8") as fh:
                fh.write(content)
        downloaded += 1
    except Exception as exc:
        print(f"  ERROR {filename}: {exc}")
        failed += 1

print(f"existing={existing_count}  downloaded={downloaded}  failed={failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
