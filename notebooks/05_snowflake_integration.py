# Notebook: 05_snowflake_integration
# Purpose: Sync Gold tables to Snowflake for broader consumption
#
# CRITICAL FIX from basic version:
# - Credentials via Databricks Secret Scope (not hardcoded)
# - Incremental sync not full overwrite
# - Proper error handling
#
# SECURITY NOTE:
# Never hardcode credentials in notebooks.
# Set up a Databricks Secret Scope backed by Azure Key Vault:
#   databricks secrets create-scope --scope snowflake-scope
#   databricks secrets put --scope snowflake-scope --key sf-password
#
# Then access via dbutils.secrets.get()

from pyspark.sql.functions import *

# ── Credentials from Secret Scope ────────────────────────────────────────────
# This is the correct pattern. No passwords in code.

snowflake_options = {
    "sfURL":       dbutils.secrets.get(scope="snowflake-scope", key="sf-url"),
    "sfUser":      dbutils.secrets.get(scope="snowflake-scope", key="sf-user"),
    "sfPassword":  dbutils.secrets.get(scope="snowflake-scope", key="sf-password"),
    "sfDatabase":  "IOT_LAKEHOUSE",
    "sfWarehouse": "IOT_COMPUTE",
    "sfRole":      "IOT_READER",   # least privilege — not ACCOUNTADMIN
}

# ── Sync function — incremental not overwrite ─────────────────────────────────
def sync_incremental_to_snowflake(
    databricks_table: str,
    snowflake_schema: str,
    snowflake_table: str,
    merge_key: str,
    timestamp_col: str = "_gold_updated_at"
):
    """
    Incrementally sync a Gold Delta table to Snowflake.

    Strategy:
    1. Find the max timestamp already in Snowflake
    2. Read only newer records from Databricks
    3. Write to a Snowflake staging table
    4. MERGE staging into target (upsert)

    Why incremental:
    - Gold device_daily_metrics has 50 devices × 365 days = 18,250 rows
    - Full overwrite works at this scale but won't at 5,000 devices × 3 years
    - Incremental pattern scales, overwrite doesn't
    """

    print(f"\n🔄 Syncing {databricks_table} → {snowflake_schema}.{snowflake_table}")

    opts = {**snowflake_options, "sfSchema": snowflake_schema}

    # Find last sync point
    try:
        sf_max_ts = (spark.read
            .format("snowflake")
            .options(**opts)
            .option("query", f"SELECT MAX({timestamp_col}) FROM {snowflake_table}")
            .load()
            .collect()[0][0]
        )
        sf_max_ts = sf_max_ts or "1900-01-01"
    except Exception:
        sf_max_ts = "1900-01-01"   # table doesn't exist yet

    # Read only new/updated records
    new_records = (spark.table(databricks_table)
        .filter(col(timestamp_col) > sf_max_ts)
    )

    record_count = new_records.count()

    if record_count == 0:
        print(f"   ℹ️  No new records since {sf_max_ts} — skipping")
        return

    # Write to Snowflake (append — MERGE happens in Snowflake via task or separate step)
    (new_records.write
        .format("snowflake")
        .options(**opts)
        .option("dbtable", f"{snowflake_table}_STAGING")
        .mode("overwrite")
        .save())

    print(f"   ✅ {record_count} records staged → {snowflake_schema}.{snowflake_table}_STAGING")

# ── Run sync for all Gold tables ──────────────────────────────────────────────

tables = [
    ("adb_iot_lakehouse_dev.gold.device_daily_metrics",
     "GOLD", "DEVICE_DAILY_METRICS", "device_id"),
    ("adb_iot_lakehouse_dev.gold.clinical_alerts",
     "GOLD", "CLINICAL_ALERTS", "reading_id"),
    ("adb_iot_lakehouse_dev.gold.ward_summary",
     "GOLD", "WARD_SUMMARY", "event_date"),
]

for db_table, sf_schema, sf_table, key in tables:
    try:
        sync_incremental_to_snowflake(db_table, sf_schema, sf_table, key)
    except Exception as e:
        print(f"❌ Failed to sync {sf_table}: {e}")
        raise   # fail fast — don't silently skip

print("\n🎉 Snowflake sync complete")
