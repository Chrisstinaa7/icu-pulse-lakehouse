# Notebook: 04_gold_aggregations
# Purpose: Build business-ready tables for clinical dashboards
#
# KEY UPGRADES from basic version:
# - Incremental processing (not overwrite)
# - Three separate Gold tables with clear business purpose
# - Alert escalation logic
# - Device health scoring
# - Z-ordering for query performance

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

vitals_silver = spark.table("adb_iot_lakehouse_dev.silver.vitals_cleaned")
alarms_silver = spark.table("adb_iot_lakehouse_dev.silver.alarms_cleaned")

# ─────────────────────────────────────────────────────────────────────────────
# Gold Table 1: device_daily_metrics
# Purpose: Daily rollup per device for trend analysis and dashboards
# Consumers: Clinical dashboard, ward management reports
# ─────────────────────────────────────────────────────────────────────────────

vitals_daily = (vitals_silver
    .groupBy("device_id", "event_date", "patient_type")
    .agg(
        # Vitals averages
        avg("heart_rate").alias("avg_heart_rate"),
        max("heart_rate").alias("max_heart_rate"),
        min("heart_rate").alias("min_heart_rate"),
        avg("bp_systolic").alias("avg_bp_systolic"),
        avg("bp_diastolic").alias("avg_bp_diastolic"),
        avg("oxygen_saturation").alias("avg_spo2"),
        min("oxygen_saturation").alias("min_spo2"),

        # Alert counts
        count("*").alias("total_readings"),
        sum(when(col("alert_level") == "CRITICAL", 1).otherwise(0)).alias("critical_alerts"),
        sum(when(col("alert_level") == "WARNING",  1).otherwise(0)).alias("warning_alerts"),
        sum(when(col("is_sudden_change"), 1).otherwise(0)).alias("sudden_changes"),

        # Data completeness
        count("heart_rate").alias("valid_hr_readings"),
        count("oxygen_saturation").alias("valid_spo2_readings"),
    )
    .withColumn("data_completeness_pct",
                col("valid_hr_readings") / col("total_readings") * 100)

    # Device health score (0-100): 100 = all normal, 0 = all critical
    .withColumn("device_health_score",
        greatest(lit(0),
            lit(100)
            - (col("critical_alerts") * lit(10))
            - (col("warning_alerts")  * lit(3))
            - (col("sudden_changes")  * lit(2))
        ))
)

alarms_daily = (alarms_silver
    .withColumn("alarm_date", to_date("timestamp"))
    .groupBy("device_id", "alarm_date")
    .agg(
        count("*").alias("alarm_count"),
        sum(when(col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alarm_count"),
        collect_list("alarm_type").alias("alarm_types"),
    )
)

gold_daily = (vitals_daily
    .join(alarms_daily,
          (vitals_daily.device_id == alarms_daily.device_id) &
          (vitals_daily.event_date == alarms_daily.alarm_date),
          "left")
    .drop(alarms_daily.device_id)
    .withColumn("_gold_updated_at", current_timestamp())
)

# Incremental merge
daily_table = "adb_iot_lakehouse_dev.gold.device_daily_metrics"

if DeltaTable.isDeltaTable(spark, daily_table):
    DeltaTable.forName(spark, daily_table).alias("t").merge(
        gold_daily.alias("s"),
        "t.device_id = s.device_id AND t.event_date = s.event_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    gold_daily.write.format("delta").mode("overwrite") \
        .partitionBy("event_date") \
        .saveAsTable(daily_table)

# Optimise for dashboard queries (filter by date and device most often)
spark.sql(f"OPTIMIZE {daily_table} ZORDER BY (device_id, event_date)")
print(f"✅ Gold: device_daily_metrics updated and optimised")

# ─────────────────────────────────────────────────────────────────────────────
# Gold Table 2: clinical_alerts
# Purpose: Real-time alert feed for nursing station
# Only CRITICAL and WARNING readings — not the full history
# ─────────────────────────────────────────────────────────────────────────────

alerts_gold = (vitals_silver
    .filter(col("alert_level").isin("CRITICAL", "WARNING"))
    .select(
        "device_id",
        "reading_id",
        "event_timestamp",
        "alert_level",
        "heart_rate",
        "heart_rate_status",
        "oxygen_saturation",
        "spo2_status",
        "bp_systolic",
        "bp_status",
        "is_sudden_change",
        "rolling_avg_heart_rate",
        "rolling_avg_spo2",
        "patient_type",
        "_silver_processed_at",
    )
    .withColumn("_gold_updated_at", current_timestamp())
)

alerts_table = "adb_iot_lakehouse_dev.gold.clinical_alerts"

if DeltaTable.isDeltaTable(spark, alerts_table):
    DeltaTable.forName(spark, alerts_table).alias("t").merge(
        alerts_gold.alias("s"),
        "t.reading_id = s.reading_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    alerts_gold.write.format("delta").mode("overwrite") \
        .partitionBy("event_date") \
        .saveAsTable(alerts_table)

spark.sql(f"OPTIMIZE {alerts_table} ZORDER BY (device_id, event_timestamp)")
print(f"✅ Gold: clinical_alerts updated — {alerts_gold.count()} alert records")

# ─────────────────────────────────────────────────────────────────────────────
# Gold Table 3: ward_summary
# Purpose: Executive/management view — ward-level KPIs
# ─────────────────────────────────────────────────────────────────────────────

ward_summary = (vitals_silver
    .withColumn("event_date", to_date("event_timestamp"))
    .groupBy("event_date")
    .agg(
        countDistinct("device_id").alias("active_devices"),
        count("*").alias("total_readings"),
        sum(when(col("alert_level") == "CRITICAL", 1).otherwise(0)).alias("total_critical_alerts"),
        sum(when(col("alert_level") == "WARNING",  1).otherwise(0)).alias("total_warnings"),
        avg("heart_rate").alias("ward_avg_heart_rate"),
        avg("oxygen_saturation").alias("ward_avg_spo2"),
        # % of readings that triggered an alert
        (sum(when(col("alert_level") != "NORMAL", 1).otherwise(0)) /
         count("*") * 100).alias("alert_rate_pct"),
    )
    .withColumn("_gold_updated_at", current_timestamp())
)

ward_table = "adb_iot_lakehouse_dev.gold.ward_summary"

if DeltaTable.isDeltaTable(spark, ward_table):
    DeltaTable.forName(spark, ward_table).alias("t").merge(
        ward_summary.alias("s"),
        "t.event_date = s.event_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    ward_summary.write.format("delta").mode("overwrite").saveAsTable(ward_table)

print(f"✅ Gold: ward_summary updated")

# ── Final validation ──────────────────────────────────────────────────────────
display(spark.sql("""
    SELECT event_date, active_devices, total_readings,
           total_critical_alerts, alert_rate_pct
    FROM adb_iot_lakehouse_dev.gold.ward_summary
    ORDER BY event_date DESC
    LIMIT 7
"""))
