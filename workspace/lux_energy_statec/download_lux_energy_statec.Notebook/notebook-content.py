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

SANDBOX_DIR = "Files/data/sandbox/lux_energy_statec"
SDMX_BASE = "https://lustat.statec.lu/rest/data/LU1,{df_id}/all?format=csvfilewithlabels"

DATAFLOWS = [
    "DF_A4500",  # Electricity market
    "DF_A4501",  # Natural gas market
    "DF_A4502",  # Electricity price composition (EUR/kWh)
    "DF_A4503",  # Natural gas price composition (EUR/MWh)
    "DF_A4504",  # Gasoline price composition (EUR/litre)
    "DF_A4505",  # Diesel/gasoil price composition (EUR/litre)
    "DF_A4506",  # LPG fuel price composition (EUR/litre)
    "DF_A4507",  # Heating energy average prices (EUR/kWh)
]

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

downloaded = 0
unchanged = 0
failed = 0

for df_id in DATAFLOWS:
    url = SDMX_BASE.format(df_id=df_id)
    dest = os.path.join(base_path, f"{df_id}.csv")

    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            content = resp.read()

        existing = open(dest, "rb").read() if os.path.exists(dest) else b""
        if content == existing:
            unchanged += 1
        else:
            with open(dest, "wb") as f:
                f.write(content)
            downloaded += 1
            print(f"  Updated {df_id}.csv ({len(content):,} bytes)")
    except Exception as e:
        print(f"  FAIL {df_id}: {e}")
        failed += 1

print(f"updated={downloaded}  unchanged={unchanged}  failed={failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
