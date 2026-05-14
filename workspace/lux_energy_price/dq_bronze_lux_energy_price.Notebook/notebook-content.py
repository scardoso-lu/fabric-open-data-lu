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

BRONZE_TABLE = "bronze_lux_energy_price"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## DQ: bronze_lux_energy_price
# Owned by the tester agent. Checks run after ingestion; raise on any FAIL.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

try:
    df = spark.read.format("delta").table(BRONZE_TABLE)
    row_count = df.count()
    print(f"Row count: {row_count:,}")
except Exception as e:
    print(f"FAIL [table_accessible]: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = "PASS" if row_count > 0 else "FAIL"
print(f"{result} [row_count > 0]: {row_count}")
if result == "FAIL":
    raise AssertionError("Bronze table is empty")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_pk = df.filter(
    F.col("source_file").isNull() |
    F.col("timeseries_mrid").isNull() |
    F.col("position").isNull()
).count()
result = "PASS" if null_pk == 0 else "FAIL"
print(f"{result} [no_null_pks]: {null_pk} null PK rows")
if result == "FAIL":
    raise AssertionError(f"Found {null_pk} rows with null PKs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total = df.count()
distinct = df.select("source_file", "timeseries_mrid", "position").distinct().count()
result = "PASS" if total == distinct else "FAIL"
print(f"{result} [no_duplicate_pks]: total={total:,} distinct={distinct:,}")
if result == "FAIL":
    raise AssertionError(f"Found {total - distinct} duplicate PK rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bad_price = df.filter(
    F.col("price_eur_mwh").isNull() |
    (F.col("price_eur_mwh") < -500) |
    (F.col("price_eur_mwh") > 4000)
).count()
result = "PASS" if bad_price == 0 else "FAIL"
print(f"{result} [price_range]: {bad_price} out-of-range price rows")
if result == "FAIL":
    raise AssertionError(f"Found {bad_price} rows with implausible prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_cols = {
    "price_datetime_utc", "price_date_lu", "price_hour_lu", "price_quarter",
    "position", "price_eur_mwh", "currency", "source_file",
    "timeseries_mrid", "classification_position", "_ingest_ts", "_ingest_date"
}
actual_cols = set(df.columns)
missing = expected_cols - actual_cols
result = "PASS" if not missing else "FAIL"
print(f"{result} [schema_match]: missing={missing or 'none'}")
if result == "FAIL":
    raise AssertionError(f"Schema missing columns: {missing}")

print("\nAll DQ checks PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
