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

# PARAMETERS CELL ********************

DATA_BASE_URL = "https://raw.githubusercontent.com/scardoso-lu/jaffle-shop-classic/main/seeds"
SANDBOX_DIR   = "Files/data/sandbox/jaffle_shop"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RAW_FILES = ["raw_customers.csv", "raw_orders.csv", "raw_payments.csv"]
RAW_URLS  = {f: f"{DATA_BASE_URL}/{f}" for f in RAW_FILES}


try:
    from notebookutils import mssparkutils
    IS_FABRIC = True
except ImportError:
    IS_FABRIC = False

import urllib.request
import os
from pathlib import Path

try:
    if IS_FABRIC:
        try:
            existing = {f.name for f in mssparkutils.fs.ls(SANDBOX_DIR)}
        except Exception:
            existing = set()
            mssparkutils.fs.mkdirs(SANDBOX_DIR)
    else:
        Path(SANDBOX_DIR).mkdir(parents=True, exist_ok=True)
        existing = set(os.listdir(SANDBOX_DIR))
except Exception as exc:
    print(f"Sandbox init failed: {exc}")
    raise

existing_count = downloaded = failed = 0

for filename, url in RAW_URLS.items():
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
        print(f"  downloaded {filename}")
    except Exception as exc:
        print(f"  ERROR {filename}: {exc}")
        failed += 1

print(f"existing={existing_count}  downloaded={downloaded}  failed={failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
