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

%pip install "great_expectations>=1.0,<2.0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_TABLE = "bronze_lux_house_prices"
BATCH_ID_FILTER = ""
ROW_COUNT_MIN = 1
EXPECTED_MAX = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dataclasses import dataclass


@dataclass(frozen=True)
class DQContract:
    table: str
    pk_columns: list[str]
    required_columns: list[str]
    non_null_columns: list[str]


CONTRACT = DQContract(
    table=BRONZE_TABLE,
    pk_columns=["record_key"],
    required_columns=[
        "record_key",
        "dataset_id",
        "dataset_slug",
        "resource_id",
        "resource_title",
        "resource_url",
        "source_file",
        "sheet_name",
        "property_type",
        "series_type",
        "year",
        "record_scope",
        "offer_count_raw",
        "announced_price_eur_current_raw",
        "announced_price_m2_eur_current_raw",
        "price_suppressed",
        "_ingest_timestamp",
        "_source_system",
        "_batch_id",
        "_ingest_date",
    ],
    non_null_columns=[
        "record_key",
        "resource_id",
        "source_file",
        "sheet_name",
        "property_type",
        "series_type",
        "record_scope",
        "_ingest_timestamp",
        "_source_system",
        "_batch_id",
        "_ingest_date",
    ],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = spark.read.format("delta").table(CONTRACT.table)
except Exception as exc:
    print(f"FAIL [table_accessible]: {exc}")
    raise

if BATCH_ID_FILTER:
    df = df.filter(F.col("_batch_id") == BATCH_ID_FILTER)

row_count = df.count()
print(f"table={CONTRACT.table}")
print(f"batch_id_filter={BATCH_ID_FILTER or '<all>'}")
print(f"row_count={row_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = gx.get_context(mode="ephemeral")
datasource = context.data_sources.add_spark("spark_ds")
asset = datasource.add_dataframe_asset("bronze_lux_house_prices")
batch_definition = asset.add_batch_definition_whole_dataframe("batch")
suite = gx.ExpectationSuite(name="dq_bronze_lux_house_prices")

max_value = EXPECTED_MAX if EXPECTED_MAX and EXPECTED_MAX > 0 else None
row_count_kwargs = {"min_value": ROW_COUNT_MIN}
if max_value is not None:
    row_count_kwargs["max_value"] = max_value
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(**row_count_kwargs))

for column in CONTRACT.required_columns:
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column=column))

for column in CONTRACT.non_null_columns:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="record_key"))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="property_type",
        value_set=["apartment", "house"],
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="series_type",
        value_set=["latest_12_months", "retrospective"],
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="record_scope",
        value_set=["commune", "national_average", "total_offers"],
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="record_key",
        regex=r"^[^\r\n]+$",
    )
)

context.suites.add(suite)
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(name="dq_bronze_lux_house_prices_run", data=batch_definition, suite=suite)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = validation_definition.run(batch_parameters={"dataframe": df})
passed = bool(result["success"])
results_list = result["results"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n=== DQ Report: bronze_lux_house_prices ===")
print(f"total rows : {row_count}")
print(f"overall    : {'PASS' if passed else 'FAIL'}")

for item in results_list:
    status = "PASS" if item["success"] else "FAIL"
    expectation_config = item["expectation_config"]
    if isinstance(expectation_config, dict):
        expectation = expectation_config.get("type", "<unknown>")
        column = expectation_config.get("kwargs", {}).get("column", "")
    else:
        expectation = getattr(expectation_config, "type", expectation_config.__class__.__name__)
        kwargs = getattr(expectation_config, "kwargs", {}) or {}
        column = kwargs.get("column", "")
    suffix = f" [{column}]" if column else ""
    print(f"  [{status}] {expectation}{suffix}")

if not passed:
    raise RuntimeError("DQ checks failed for bronze_lux_house_prices")

print("All DQ checks PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
