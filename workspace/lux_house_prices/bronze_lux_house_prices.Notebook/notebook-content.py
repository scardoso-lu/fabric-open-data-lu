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

%pip install "pandas>=2,<4" "openpyxl>=3.1,<4" "xlrd>=2,<3"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FABRIC_STAGING_DIR = "Files/data/sandbox/lux_house_prices/raw"
LOCAL_STAGING_DIR = "data/sandbox/lux_house_prices/raw"
BRONZE_TABLE = "bronze_lux_house_prices"
INGEST_DATE_OVERRIDE = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeContract:
    source_system: str
    grain: str
    primary_keys: list[str]
    bronze_table: str
    sensitive_fields: list[str]
    sensitivity: str


CONTRACT = BronzeContract(
    source_system="DATA_PUBLIC_LU_HOUSE_PRICES_BY_COMMUNE",
    grain="one advertised housing price statistic row by resource, sheet, and commune or summary row",
    primary_keys=["record_key"],
    bronze_table=BRONZE_TABLE,
    sensitive_fields=[],
    sensitivity="public",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import pathlib
import re
import uuid
from datetime import datetime, timezone
from typing import Any
import unicodedata

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

try:
    from notebookutils import mssparkutils

    IS_FABRIC = True
except ImportError:
    mssparkutils = None
    IS_FABRIC = False

SOURCE_DIR = FABRIC_STAGING_DIR if IS_FABRIC else LOCAL_STAGING_DIR
BATCH_ID = str(uuid.uuid4())
INGEST_TS = datetime.now(timezone.utc)
INGEST_DATE = (
    datetime.fromisoformat(INGEST_DATE_OVERRIDE).date()
    if INGEST_DATE_OVERRIDE
    else INGEST_TS.date()
)


def io_path(path: str) -> str:
    if IS_FABRIC and path.startswith("Files/"):
        return f"/lakehouse/default/{path}"
    return path


SOURCE_IO_DIR = io_path(SOURCE_DIR)
print(f"source_dir={SOURCE_DIR}")
print(f"source_io_dir={SOURCE_IO_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_SCHEMA = StructType(
    [
        StructField("record_key", StringType(), False),
        StructField("dataset_id", StringType(), True),
        StructField("dataset_slug", StringType(), True),
        StructField("dataset_title", StringType(), True),
        StructField("dataset_last_update", StringType(), True),
        StructField("dataset_license", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("resource_title", StringType(), True),
        StructField("resource_url", StringType(), True),
        StructField("resource_format", StringType(), True),
        StructField("resource_last_modified", StringType(), True),
        StructField("resource_checksum_md5", StringType(), True),
        StructField("source_file", StringType(), False),
        StructField("sheet_name", StringType(), False),
        StructField("property_type", StringType(), False),
        StructField("series_type", StringType(), False),
        StructField("year", StringType(), True),
        StructField("period_label", StringType(), True),
        StructField("record_scope", StringType(), False),
        StructField("commune", StringType(), True),
        StructField("offer_count_raw", StringType(), True),
        StructField("announced_price_eur_current_raw", StringType(), True),
        StructField("announced_price_m2_eur_current_raw", StringType(), True),
        StructField("price_suppressed", StringType(), False),
        StructField("_ingest_timestamp", TimestampType(), False),
        StructField("_source_system", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("_ingest_date", DateType(), False),
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_text(value: Any) -> str:
    text = clean_text(value) or ""
    text = "".join(
        char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char)
    )
    return text.lower()


def raw_text(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    return text


def find_column(labels: list[str], predicate) -> int:
    for index, label in enumerate(labels):
        if predicate(label):
            return index
    raise ValueError(f"Could not find expected column in labels: {labels}")


def infer_property_type(filename: str, resource_title: str) -> str:
    probe = normalize_text(f"{filename} {resource_title}")
    if "maison" in probe:
        return "house"
    if "appartement" in probe:
        return "apartment"
    return "unknown"


def infer_series_type(filename: str, resource_title: str) -> str:
    probe = normalize_text(f"{filename} {resource_title}")
    if "retrospective" in probe or re.search(r"20\d{2}[-_]20\d{2}", filename):
        return "retrospective"
    return "latest_12_months"


def infer_year(filename: str, sheet_name: str) -> str | None:
    sheet_match = re.search(r"(20\d{2})", sheet_name)
    if sheet_match:
        return sheet_match.group(1)
    matches = re.findall(r"20\d{2}", filename)
    return matches[-1] if matches else None


def load_metadata(filename: str) -> dict:
    path = pathlib.Path(SOURCE_IO_DIR) / f"{filename}.metadata.json"
    if not path.exists():
        return {"dataset": {}, "resource": {}}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def existing_source_files() -> set[str]:
    try:
        df = spark.read.format("delta").table(CONTRACT.bronze_table)
        files = {row.source_file for row in df.select("source_file").distinct().collect()}
        print(f"bronze_existing_source_files={len(files)}")
        return files
    except Exception:
        print("Bronze table not found; processing all staged Excel files")
        return set()


def parse_sheet(filename: str, sheet_name: str, raw_df: pd.DataFrame, metadata: dict) -> list[dict]:
    header_index = None
    header_labels: list[str] = []
    for idx, row in raw_df.iterrows():
        labels = [normalize_text(value) for value in row.tolist()]
        if "commune" in labels and any("nombre" in label and "offre" in label for label in labels):
            header_index = idx
            header_labels = labels
            break

    if header_index is None:
        return []

    commune_col = find_column(header_labels, lambda label: label == "commune")
    offer_col = find_column(header_labels, lambda label: "nombre" in label and "offre" in label)
    price_col = find_column(
        header_labels,
        lambda label: "prix moyen annonce" in label and "m2" not in label,
    )
    price_m2_col = find_column(header_labels, lambda label: "prix moyen annonce" in label and "m2" in label)

    dataset = metadata.get("dataset") or {}
    resource = metadata.get("resource") or {}
    resource_title = resource.get("title") or ""
    property_type = infer_property_type(filename, resource_title)
    series_type = infer_series_type(filename, resource_title)
    year = infer_year(filename, sheet_name)
    checksum = resource.get("checksum") or {}

    rows: list[dict] = []
    for _, row in raw_df.iloc[header_index + 1 :].iterrows():
        label = clean_text(row.iloc[commune_col])
        if not label:
            continue
        if normalize_text(label).startswith("source :"):
            break

        norm_label = normalize_text(label)
        if norm_label.startswith("moyenne nationale"):
            record_scope = "national_average"
            commune = None
        elif norm_label.startswith("total d'offres"):
            record_scope = "total_offers"
            commune = None
        else:
            record_scope = "commune"
            commune = label

        offer_count = raw_text(row.iloc[offer_col])
        price = raw_text(row.iloc[price_col])
        price_m2 = raw_text(row.iloc[price_m2_col])
        suppressed = "true" if "*" in {offer_count, price, price_m2} else "false"
        key_label = commune or record_scope
        record_key = "|".join([filename, sheet_name, record_scope, key_label])

        rows.append(
            {
                "record_key": record_key,
                "dataset_id": dataset.get("id"),
                "dataset_slug": dataset.get("slug"),
                "dataset_title": dataset.get("title"),
                "dataset_last_update": dataset.get("last_update"),
                "dataset_license": dataset.get("license"),
                "resource_id": resource.get("id"),
                "resource_title": resource_title,
                "resource_url": resource.get("url"),
                "resource_format": resource.get("format"),
                "resource_last_modified": resource.get("last_modified"),
                "resource_checksum_md5": checksum.get("value") if checksum.get("type") == "md5" else None,
                "source_file": filename,
                "sheet_name": sheet_name,
                "property_type": property_type,
                "series_type": series_type,
                "year": year,
                "period_label": resource.get("description"),
                "record_scope": record_scope,
                "commune": commune,
                "offer_count_raw": offer_count,
                "announced_price_eur_current_raw": price,
                "announced_price_m2_eur_current_raw": price_m2,
                "price_suppressed": suppressed,
                "_ingest_timestamp": INGEST_TS,
                "_source_system": CONTRACT.source_system,
                "_batch_id": BATCH_ID,
                "_ingest_date": INGEST_DATE,
            }
        )
    return rows


def parse_excel_file(filename: str) -> list[dict]:
    file_path = pathlib.Path(SOURCE_IO_DIR) / filename
    metadata = load_metadata(filename)
    workbook = pd.ExcelFile(file_path)
    rows: list[dict] = []
    for sheet_name in workbook.sheet_names:
        raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=object)
        sheet_rows = parse_sheet(filename, sheet_name, raw_df, metadata)
        rows.extend(sheet_rows)
        print(f"parsed {filename}::{sheet_name} rows={len(sheet_rows)}")
    return rows


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not os.path.isdir(SOURCE_IO_DIR):
    raise FileNotFoundError(f"Source directory does not exist: {SOURCE_IO_DIR}")

already_ingested = existing_source_files()
excel_files = sorted(
    filename
    for filename in os.listdir(SOURCE_IO_DIR)
    if filename.lower().endswith((".xls", ".xlsx")) and filename not in already_ingested
)

print(f"new_files_to_process={len(excel_files)}")

all_rows: list[dict] = []
for filename in excel_files:
    all_rows.extend(parse_excel_file(filename))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not all_rows:
    print("No new rows to write; Bronze is up to date")
    try:
        total = spark.read.format("delta").table(CONTRACT.bronze_table).count()
    except Exception:
        total = 0
    print(f"table_row_count={total}")
else:
    new_df = spark.createDataFrame(all_rows, schema=BRONZE_SCHEMA)
    replace_files = sorted({row["source_file"] for row in all_rows})
    replace_expr = "source_file IN (" + ", ".join(sql_quote(name) for name in replace_files) + ")"

    (
        new_df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", replace_expr)
        .partitionBy("source_file")
        .saveAsTable(CONTRACT.bronze_table)
    )

    rows_written = new_df.count()
    table_count = spark.read.format("delta").table(CONTRACT.bronze_table).count()
    print(f"new_files_processed={len(replace_files)} rows_written={rows_written} table_row_count={table_count}")
    print(f"batch_id={BATCH_ID} ingest_date={INGEST_DATE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
