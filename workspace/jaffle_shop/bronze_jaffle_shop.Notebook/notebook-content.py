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

SANDBOX_DIR = "Files/data/sandbox/jaffle_shop"

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

from pyspark.sql import functions as F

# Spark's distributed reader needs the Lakehouse-relative path (Files/...).
# The POSIX mount /lakehouse/default/... only works for Python file I/O, not spark.read.
sandbox_path = SANDBOX_DIR if IS_FABRIC else SANDBOX_DIR
print(f"Sandbox: {sandbox_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all columns as strings first, then cast explicitly.
# This is safer than PySpark's CSV schema enforcement which silently drops bad rows.

def read_seed(name: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{sandbox_path}/{name}.csv")
    )

try:
    raw_customers = read_seed("raw_customers")
    raw_orders    = read_seed("raw_orders")
    raw_payments  = read_seed("raw_payments")
    print("CSVs loaded")
    raw_customers.printSchema()
    raw_orders.printSchema()
    raw_payments.printSchema()
except Exception as exc:
    print(f"CSV read failed: {exc}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cast and enrich each seed table, then overwrite the Delta table.

def write_table(df, table_name: str) -> None:
    count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"{table_name}: {count:,} rows written")

try:
    customers = (
        raw_customers
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
        )
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.lit("raw_customers.csv"))
    )
    write_table(customers, "jaffle_raw_customers")

    orders = (
        raw_orders
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
            F.col("status").cast("string").alias("status"),
        )
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.lit("raw_orders.csv"))
    )
    write_table(orders, "jaffle_raw_orders")

    payments = (
        raw_payments
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("order_id").cast("int").alias("order_id"),
            F.col("payment_method").cast("string").alias("payment_method"),
            # Amounts are in cents as integers; store as decimal for SQL compatibility
            F.col("amount").cast("decimal(10,2)").alias("amount"),
        )
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.lit("raw_payments.csv"))
    )
    write_table(payments, "jaffle_raw_payments")

except Exception as exc:
    print(f"Write failed: {exc}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
