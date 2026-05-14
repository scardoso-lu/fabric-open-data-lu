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
BRONZE_TABLE = "bronze_lux_energy_statec"

DATAFLOW_META = {
    "DF_A4500": {"energy_source": "electricity",    "market_type": "market"},
    "DF_A4501": {"energy_source": "natural_gas",    "market_type": "market"},
    "DF_A4502": {"energy_source": "electricity",    "market_type": "price"},
    "DF_A4503": {"energy_source": "natural_gas",    "market_type": "price"},
    "DF_A4504": {"energy_source": "gasoline",       "market_type": "price"},
    "DF_A4505": {"energy_source": "diesel",         "market_type": "price"},
    "DF_A4506": {"energy_source": "lpg",            "market_type": "price"},
    "DF_A4507": {"energy_source": "heating_mix",    "market_type": "price"},
}

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
import re
import csv
import io
from datetime import date

if IS_FABRIC:
    sandbox_path = f"/lakehouse/default/{SANDBOX_DIR}"
else:
    sandbox_path = SANDBOX_DIR

print(f"Sandbox: {sandbox_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_unit(structure_name: str) -> str:
    m = re.search(r"in (EUR[^)]+)", structure_name, re.IGNORECASE)
    return m.group(1).strip() if m else ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_csv(filepath: str, df_id: str) -> list[dict]:
    with open(filepath, encoding="utf-8-sig") as f:
        content = f.read()

    reader = csv.DictReader(io.StringIO(content))
    rows = []
    unit = ""
    meta = DATAFLOW_META.get(df_id, {"energy_source": "unknown", "market_type": "unknown"})

    for raw in reader:
        # Normalise varying dimension column: SPECIFICATION or PRICE → indicator
        indicator_code = raw.get("SPECIFICATION") or raw.get("PRICE") or ""
        indicator_label = raw.get("Specification") or raw.get("Price") or ""

        obs_raw = raw.get("OBS_VALUE", "").strip()
        obs_value = float(obs_raw) if obs_raw else None

        if not unit:
            unit = extract_unit(raw.get("STRUCTURE_NAME", ""))

        rows.append({
            "dataflow_id":      df_id,
            "dataflow_name":    raw.get("STRUCTURE_NAME", "").strip(),
            "energy_source":    meta["energy_source"],
            "market_type":      meta["market_type"],
            "indicator_code":   indicator_code.strip(),
            "indicator_label":  indicator_label.strip(),
            "freq_code":        raw.get("FREQ", "").strip(),
            "freq_label":       raw.get("Frequency", "").strip(),
            "time_period":      raw.get("TIME_PERIOD", "").strip(),
            "obs_value":        obs_value,
            "obs_status":       raw.get("OBS_STATUS", "").strip(),
            "unit":             unit,
        })

    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType
)

schema = StructType([
    StructField("dataflow_id",     StringType()),
    StructField("dataflow_name",   StringType()),
    StructField("energy_source",   StringType()),
    StructField("market_type",     StringType()),
    StructField("indicator_code",  StringType()),
    StructField("indicator_label", StringType()),
    StructField("freq_code",       StringType()),
    StructField("freq_label",      StringType()),
    StructField("time_period",     StringType()),
    StructField("obs_value",       DoubleType()),
    StructField("obs_status",      StringType()),
    StructField("unit",            StringType()),
])

all_rows = []
file_counts = {}

for df_id in DATAFLOW_META:
    fpath = os.path.join(sandbox_path, f"{df_id}.csv")
    if not os.path.exists(fpath):
        print(f"  MISSING {df_id}.csv — skipping")
        continue
    try:
        rows = parse_csv(fpath, df_id)
        all_rows.extend(rows)
        file_counts[df_id] = len(rows)
        print(f"  {df_id}: {len(rows)} rows")
    except Exception as e:
        print(f"  FAIL {df_id}: {e}")
        raise

print(f"Total rows parsed: {len(all_rows):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

new_df = (
    spark.createDataFrame(all_rows, schema=schema)
    .withColumn("_ingest_ts",   F.current_timestamp())
    .withColumn("_ingest_date", F.current_date())
)

try:
    (
        new_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("dataflow_id")
        .saveAsTable(BRONZE_TABLE)
    )
    total = spark.read.format("delta").table(BRONZE_TABLE).count()
    print(f"rows written={len(all_rows):,}  table row count={total:,}")
except Exception as e:
    print(f"Bronze write failed: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
