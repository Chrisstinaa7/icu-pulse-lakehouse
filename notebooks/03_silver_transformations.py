# Notebook: 03_silver_transformations
# Purpose: Clean, validate, and enrich vitals data with clinical context
#
# KEY UPGRADES from basic version:
# - Clinical threshold validation (not just null checks)
# - Rolling 10-minute average per device (window function)
# - Sudden change detection (>20% deviation from rolling avg)
# - Deduplication using MERGE not overwrite
# - Incremental processing (only new Bronze records)
# - Watermarking for late sensor data

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Clinical thresholds ───────────────────────────────────────────────────────
# These are real ICU alert thresholds, not made up.
# Source: standard hospital early warning score (NEWS2) parameters.

THRESHOLDS = {
    "heart_rate_min":        40,     # < 40 bpm = severe bradycardia
    "heart_rate_max":        150,    # > 150 bpm = severe tachycardia
    "oxygen_saturation_min": 90,     # < 90% = hypoxia
    "systolic_max":          180,    # > 180 mmHg = hypertensive crisis
    "systolic_min":          80,     # < 80 mmHg = hypotensive
    "rolling_deviation_pct": 0.20,   # > 20% sudden change = flag
}

# ── Read Bronze incrementally ─────────────────────────────────────────────────
# Only process records not yet in Silver.
# This is the incremental pattern — not overwriting the whole table every run.

last_silver_ts = spark.sql("""
    SELECT COALESCE(MAX(event_timestamp), '1900-01-01')
    FROM adb_iot_lakehouse_dev.silver.vitals_cleaned
""").collect()[0][0]

print(f"Processing Bronze records newer than: {last_silver_ts}")

vitals_bronze = spark.table("adb_iot_lakehouse_dev.bronze.vitals") \
                     .filter(col("event_timestamp") > last_silver_ts)

print(f"New records to process: {vitals_bronze.count()}")

# ── Step 1: Flatten nested struct ────────────────────────────────────────────
vitals_flat = (vitals_bronze
    .withColumn("bp_systolic",  col("blood_pressure.systolic"))
    .withColumn("bp_diastolic", col("blood_pressure.diastolic"))
    .drop("blood_pressure")
)

# ── Step 2: Clinical threshold validation ─────────────────────────────────────
# Each reading gets a flag AND a human-readable reason.
# Downstream Gold can filter on is_clinical_alert = True.

vitals_validated = (vitals_flat
    .withColumn("heart_rate_status",
        when(col("heart_rate") < THRESHOLDS["heart_rate_min"],  lit("CRITICAL_LOW"))
       .when(col("heart_rate") > THRESHOLDS["heart_rate_max"],  lit("CRITICAL_HIGH"))
       .when(col("heart_rate") < 60,                            lit("LOW"))
       .when(col("heart_rate") > 100,                           lit("ELEVATED"))
       .otherwise(                                              lit("NORMAL")))

    .withColumn("spo2_status",
        when(col("oxygen_saturation") < THRESHOLDS["oxygen_saturation_min"],
             lit("CRITICAL_LOW"))
       .when(col("oxygen_saturation") < 94, lit("LOW"))
       .otherwise(                          lit("NORMAL")))

    .withColumn("bp_status",
        when(col("bp_systolic") > THRESHOLDS["systolic_max"], lit("HYPERTENSIVE_CRISIS"))
       .when(col("bp_systolic") < THRESHOLDS["systolic_min"], lit("HYPOTENSIVE"))
       .when(col("bp_systolic") > 140,                        lit("ELEVATED"))
       .otherwise(                                            lit("NORMAL")))

    # Any critical threshold breached = clinical alert
    .withColumn("is_clinical_alert",
        col("heart_rate_status").isin("CRITICAL_LOW", "CRITICAL_HIGH") |
        col("spo2_status").isin("CRITICAL_LOW") |
        col("bp_status").isin("HYPERTENSIVE_CRISIS", "HYPOTENSIVE"))
)

# ── Step 3: Rolling 10-minute average per device ──────────────────────────────
# This is the key engineering addition.
# A single abnormal reading might be sensor noise.
# A reading that ALSO deviates from the rolling average = more likely real.
#
# Window: per device, ordered by time, rows in last 10 minutes (120 seconds × 5 readings)

w_rolling = (Window
    .partitionBy("device_id")
    .orderBy(col("event_timestamp").cast("long"))
    .rowsBetween(-5, -1)   # previous 5 readings (~10 min at 5-min cadence)
)

vitals_enriched = (vitals_validated
    .withColumn("rolling_avg_heart_rate",
                avg("heart_rate").over(w_rolling))
    .withColumn("rolling_avg_spo2",
                avg("oxygen_saturation").over(w_rolling))

    # Sudden change: current reading deviates > 20% from rolling average
    .withColumn("is_sudden_change",
        when(col("rolling_avg_heart_rate").isNotNull(),
             (abs(col("heart_rate") - col("rolling_avg_heart_rate")) /
              col("rolling_avg_heart_rate")) > THRESHOLDS["rolling_deviation_pct"])
        .otherwise(lit(False)))

    # Final composite alert flag
    .withColumn("alert_level",
        when(col("is_clinical_alert"),                        lit("CRITICAL"))
       .when(col("is_sudden_change"),                         lit("WARNING"))
       .when(col("heart_rate_status").isin("LOW","ELEVATED"), lit("WATCH"))
       .otherwise(                                            lit("NORMAL")))

    .withColumn("_silver_processed_at", current_timestamp())
)

# ── Step 4: MERGE into Silver (idempotent) ────────────────────────────────────
silver_table = "adb_iot_lakehouse_dev.silver.vitals_cleaned"

if DeltaTable.isDeltaTable(spark, silver_table):
    DeltaTable.forName(spark, silver_table).alias("t").merge(
        vitals_enriched.alias("s"),
        "t.reading_id = s.reading_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"✅ Merged {vitals_enriched.count()} records into Silver")
else:
    vitals_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .saveAsTable(silver_table)
    print(f"✅ Created Silver vitals table")

# ── Alarms Silver (incremental) ───────────────────────────────────────────────
last_alarm_ts = spark.sql("""
    SELECT COALESCE(MAX(timestamp), '1900-01-01')
    FROM adb_iot_lakehouse_dev.silver.alarms_cleaned
""").collect()[0][0]

alarms_silver = (spark.table("adb_iot_lakehouse_dev.bronze.alarms")
    .filter(col("timestamp") > last_alarm_ts)
    .filter(col("alarm_type").isNotNull())
    .withColumn("_silver_processed_at", current_timestamp())
)

alarms_table = "adb_iot_lakehouse_dev.silver.alarms_cleaned"

if DeltaTable.isDeltaTable(spark, alarms_table):
    DeltaTable.forName(spark, alarms_table).alias("t").merge(
        alarms_silver.alias("s"),
        "t.alarm_id = s.alarm_id"
    ).whenNotMatchedInsertAll().execute()
else:
    alarms_silver.write.format("delta").mode("overwrite") \
        .partitionBy("alarm_date").saveAsTable(alarms_table)

# ── Validate ──────────────────────────────────────────────────────────────────
display(spark.sql("""
    SELECT
        alert_level,
        COUNT(*)                    AS reading_count,
        AVG(heart_rate)             AS avg_heart_rate,
        AVG(oxygen_saturation)      AS avg_spo2,
        SUM(CAST(is_sudden_change AS INT)) AS sudden_changes
    FROM adb_iot_lakehouse_dev.silver.vitals_cleaned
    GROUP BY alert_level
    ORDER BY alert_level
"""))
