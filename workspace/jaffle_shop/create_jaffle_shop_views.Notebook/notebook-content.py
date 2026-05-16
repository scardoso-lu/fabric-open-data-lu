# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "ca96d6a3-522e-b380-46ea-015d8408d2b7",
# META       "known_warehouses": [
# META         {
# META           "id": "ca96d6a3-522e-b380-46ea-015d8408d2b7",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

WAREHOUSE = "DATA_WAREHOUSE"
LAKEHOUSE  = "DATALAKE"

VIEWS = {
    "raw_customers": "jaffle_raw_customers",
    "raw_orders":    "jaffle_raw_orders",
    "raw_payments":  "jaffle_raw_payments",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import notebookutils

for view_name, lh_table in VIEWS.items():
    conn = notebookutils.data.connect_to_artifact(WAREHOUSE, artifact_type="Warehouse")
    sql = f"CREATE OR ALTER VIEW dbo.[{view_name}] AS SELECT * FROM [{LAKEHOUSE}].[dbo].[{lh_table}]"
    conn.query(sql)

print("All views created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
