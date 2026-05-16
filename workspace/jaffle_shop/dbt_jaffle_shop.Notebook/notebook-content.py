# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
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

# CELL ********************

%pip install "dbt-fabric>=1.8,<2.0" pyodbc "protobuf>=3.12.0,<6"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

WAREHOUSE      = "DATA_WAREHOUSE"
WAREHOUSE_HOST = ""
DBT_REPO_URL   = "https://github.com/scardoso-lu/jaffle-shop-classic.git"
DBT_DIR        = "/tmp/jaffle_shop_dbt"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import base64, json, notebookutils, os, subprocess, sys, textwrap, time
from pathlib import Path

_dbt = str(Path(sys.executable).parent / "dbt")

import pyodbc
_drivers = pyodbc.drivers()
print(f"ODBC drivers: {_drivers}")
_SQL_DRIVER = next(
    (d for d in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"] if d in _drivers),
    None,
)
if not _SQL_DRIVER:
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {_drivers}")
print(f"Using driver: {_SQL_DRIVER}")

# notebookutils.credentials.getToken works in API-triggered sessions;
# DefaultAzureCredential/IMDS does NOT work in Python kernel subprocesses.
_token = notebookutils.credentials.getToken("https://database.windows.net/")
print(f"Token   : obtained ({len(_token)} chars)")

try:
    _payload = _token.split('.')[1]
    _payload += '=' * (-len(_payload) % 4)
    _token_expiry = json.loads(base64.b64decode(_payload))['exp']
except Exception:
    _token_expiry = int(time.time()) + 3600

print(f"Host    : {WAREHOUSE_HOST}")
print(f"Database: {WAREHOUSE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

dbt_path = Path(DBT_DIR)

if dbt_path.exists():
    result = subprocess.run(
        ["git", "-C", DBT_DIR, "pull", "--ff-only"],
        capture_output=True, text=True,
    )
    print("git pull:", result.stdout.strip() or result.stderr.strip())
else:
    result = subprocess.run(
        ["git", "clone", "--depth", "1", DBT_REPO_URL, DBT_DIR],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise RuntimeError(f"git clone failed (exit {result.returncode})")
    print(f"Cloned into {DBT_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Inject profiles.yml at runtime — not committed to either repo.
profiles_yml = textwrap.dedent(f"""\
    jaffle_shop:
      target: fabric-dev
      outputs:
        fabric-dev:
          type: fabric
          driver: {_SQL_DRIVER}
          server: {WAREHOUSE_HOST}
          port: 1433
          database: {WAREHOUSE}
          schema: dbo
          threads: 4
          authentication: ActiveDirectoryAccessToken
          access_token: {_token}
          access_token_expires_on: {_token_expiry}
          encrypt: true
          trust_cert: true
""")
(dbt_path / "profiles.yml").write_text(profiles_yml)
print("profiles.yml written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

env = {**os.environ, "DBT_PROFILES_DIR": DBT_DIR}

result = subprocess.run(
    [_dbt, "debug", "--no-version-check",
     "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
    capture_output=True, text=True, env=env,
)
print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr)
    raise RuntimeError(f"dbt debug failed (exit {result.returncode})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

result = subprocess.run(
    [_dbt, "run", "--no-version-check",
     "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
    capture_output=True, text=True, env=env,
)
print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr)
    raise RuntimeError(f"dbt run failed (exit {result.returncode})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

result = subprocess.run(
    [_dbt, "test", "--no-version-check",
     "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
     "--exclude", "resource_type:seed"],
    capture_output=True, text=True, env=env,
)
print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr)
    raise RuntimeError(f"dbt test failed (exit {result.returncode})")

print("dbt run + test: ALL PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
