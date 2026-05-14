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

BRONZE_TABLE = "bronze_lux_energy_statec"
EXPECTED_DATAFLOWS = 8

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## DQ: bronze_lux_energy_statec
# Owned by the tester agent. Checks run after ingestion; raise on any FAIL.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

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

dataflow_count = df.select("dataflow_id").distinct().count()
result = "PASS" if dataflow_count == EXPECTED_DATAFLOWS else "FAIL"
print(f"{result} [all_dataflows_present]: {dataflow_count}/{EXPECTED_DATAFLOWS}")
if result == "FAIL":
    missing = {f"DF_A450{i}" for i in range(8)} - {r.dataflow_id for r in df.select("dataflow_id").distinct().collect()}
    raise AssertionError(f"Missing dataflows: {missing}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_pk = df.filter(
    F.col("dataflow_id").isNull() |
    F.col("indicator_code").isNull() |
    F.col("freq_code").isNull() |
    F.col("time_period").isNull()
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

non_null = df.filter(F.col("obs_value").isNotNull()).count()
rate = non_null / row_count if row_count else 0
result = "PASS" if rate > 0.5 else "FAIL"
print(f"{result} [obs_value_fill_rate]: {rate:.1%} ({non_null:,}/{row_count:,} non-null)")
if result == "FAIL":
    raise AssertionError(f"obs_value fill rate too low: {rate:.1%}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_cols = {
    "dataflow_id", "dataflow_name", "energy_source", "market_type",
    "indicator_code", "indicator_label", "freq_code", "freq_label",
    "time_period", "obs_value", "obs_status", "unit",
    "_ingest_ts", "_ingest_date"
}
missing_cols = expected_cols - set(df.columns)
result = "PASS" if not missing_cols else "FAIL"
print(f"{result} [schema_match]: missing={missing_cols or 'none'}")
if result == "FAIL":
    raise AssertionError(f"Schema missing columns: {missing_cols}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bad_period = df.filter(
    ~F.col("time_period").rlike(r"^\d{4}(-S[12]|-Q[1-4]|-[A-Z]\d+)?$")
).count()
result = "PASS" if bad_period == 0 else "FAIL"
print(f"{result} [time_period_format]: {bad_period} malformed time_period rows")
if result == "FAIL":
    raise AssertionError(f"Found {bad_period} rows with malformed time_period")

print("\nAll DQ checks PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
