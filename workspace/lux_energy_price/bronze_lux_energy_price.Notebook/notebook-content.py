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

SANDBOX_DIR = "Files/data/sandbox/lux_energy_price"
BRONZE_TABLE = "bronze_lux_energy_price"

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytz

LU_TZ = pytz.timezone("Europe/Luxembourg")

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

from pyspark.sql import functions as F

try:
    existing_df = spark.read.format("delta").table(BRONZE_TABLE)
    already_ingested = {
        r.source_file for r in existing_df.select("source_file").distinct().collect()
    }
    print(f"Bronze already contains {len(already_ingested)} source file(s)")
except Exception:
    already_ingested = set()
    print("Bronze table not yet created — will process all files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_resolution_minutes(resolution: str) -> int:
    if resolution == "PT15M":
        return 15
    if resolution == "PT60M" or resolution == "PT1H":
        return 60
    if resolution == "PT30M":
        return 30
    raise ValueError(f"Unknown resolution: {resolution}")


def parse_xml_file(filepath: str) -> list[dict]:
    tree = ET.parse(filepath)
    root = tree.getroot()

    # Detect namespace from root tag — older files may use a different version
    ns = root.tag.split("}")[0].lstrip("{") if root.tag.startswith("{") else ""

    def tag(name):
        return f"{{{ns}}}{name}" if ns else name

    doc_mrid = root.find(tag("mRID"))
    doc_mrid_val = doc_mrid.text.strip() if doc_mrid is not None else ""
    source_file = os.path.basename(filepath)
    rows = []

    for ts in root.findall(tag("TimeSeries")):
        ts_mrid_el = ts.find(tag("mRID"))
        ts_mrid = ts_mrid_el.text.strip() if ts_mrid_el is not None else ""

        cls_el = ts.find(tag("classificationSequence_AttributeInstanceComponent.position"))
        classification_pos = int(cls_el.text.strip()) if cls_el is not None else 0

        currency_el = ts.find(tag("currency_Unit.name"))
        currency = currency_el.text.strip() if currency_el is not None else "EUR"

        period = ts.find(tag("Period"))
        if period is None:
            continue

        ti = period.find(tag("timeInterval"))
        start_str = ti.find(tag("start")).text.strip()
        end_str = ti.find(tag("end")).text.strip()

        period_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        period_end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

        resolution_str = period.find(tag("resolution")).text.strip()
        res_minutes = parse_resolution_minutes(resolution_str)

        for point in period.findall(tag("Point")):
            pos_el = point.find(tag("position"))
            price_el = point.find(tag("price.amount"))
            if pos_el is None or price_el is None:
                continue

            position = int(pos_el.text.strip())
            price = float(price_el.text.strip())

            price_dt_utc = period_start + timedelta(minutes=res_minutes * (position - 1))
            price_dt_lu = price_dt_utc.astimezone(LU_TZ)

            rows.append({
                "price_datetime_utc": price_dt_utc.replace(tzinfo=None),
                "price_date_lu": price_dt_lu.date().isoformat(),
                "price_hour_lu": price_dt_lu.hour,
                "price_quarter": (price_dt_lu.minute // 15) + 1,
                "position": position,
                "price_eur_mwh": price,
                "currency": currency,
                "timeseries_mrid": ts_mrid,
                "classification_position": classification_pos,
                "resolution": resolution_str,
                "period_start_utc": period_start.replace(tzinfo=None),
                "period_end_utc": period_end.replace(tzinfo=None),
                "document_mrid": doc_mrid_val,
                "source_file": source_file,
            })

    return rows


xml_files = sorted(
    f for f in os.listdir(sandbox_path)
    if f.endswith(".xml") and f not in already_ingested
)
print(f"New XML files to process: {len(xml_files)}")

all_rows = []
skipped = 0
for fname in xml_files:
    fpath = os.path.join(sandbox_path, fname)
    try:
        rows = parse_xml_file(fpath)
        all_rows.extend(rows)
        if rows:
            print(f"  Parsed {fname}: {len(rows)} rows")
    except ET.ParseError as e:
        print(f"  SKIP {fname} (corrupt XML): {e}")
        skipped += 1
    except Exception as e:
        print(f"  FAIL {fname}: {e}")
        raise

if skipped:
    print(f"Skipped {skipped} corrupt file(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)
import pyspark.sql.functions as F

if not all_rows:
    print("No new rows to write — Bronze is up to date")
    total = spark.read.format("delta").table(BRONZE_TABLE).count() if already_ingested else 0
    print(f"Bronze table row count: {total:,}")
else:
    schema = StructType([
        StructField("price_datetime_utc", TimestampType()),
        StructField("price_date_lu", StringType()),
        StructField("price_hour_lu", IntegerType()),
        StructField("price_quarter", IntegerType()),
        StructField("position", IntegerType()),
        StructField("price_eur_mwh", DoubleType()),
        StructField("currency", StringType()),
        StructField("timeseries_mrid", StringType()),
        StructField("classification_position", IntegerType()),
        StructField("resolution", StringType()),
        StructField("period_start_utc", TimestampType()),
        StructField("period_end_utc", TimestampType()),
        StructField("document_mrid", StringType()),
        StructField("source_file", StringType()),
    ])

    new_df = (
        spark.createDataFrame(all_rows, schema=schema)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.col("price_date_lu")))
    )

    try:
        (
            new_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(BRONZE_TABLE)
        )
        rows_written = new_df.count()
        total = spark.read.format("delta").table(BRONZE_TABLE).count()
        print(f"new files processed={len(xml_files)}  rows written={rows_written:,}  table row count={total:,}")
    except Exception as e:
        print(f"Bronze write failed: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
