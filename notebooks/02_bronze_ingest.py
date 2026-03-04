# Notebook: 02_bronze_ingest
# Purpose: Incrementally ingest sensor files into Bronze Delta tables
#
# KEY UPGRADES from basic version:
# - Auto Loader instead of spark.read (incremental, not full reload)
# - Deduplication using reading_id (handles sensor double-sends)
# - Explicit schema enforcement (don't infer — control what enters Bronze)
# - Partitioned by date for efficient downstream reads
# - Quarantine bad records rather than silently dropping
# - Checkpointing for fault tolerance

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ── Paths ─────────────────────────────────────────────────────────────────────
VITALS_LANDING    = "abfss://bronze@iotlakehousestorage.dfs.core.windows.net/healthcare_iot/vitals/"
ALARMS_LANDING    = "abfss://bronze@iotlakehousestorage.dfs.core.windows.net/healthcare_iot/alarms/"
CHECKPOINT_BASE   = "abfss://checkpoints@iotlakehousestorage.dfs.core.windows.net/"
QUARANTINE_PATH   = "abfss://quarantine@iotlakehousestorage.dfs.core.windows.net/vitals/"

# ── Schemas ───────────────────────────────────────────────────────────────────
# WHY explicit schema: Auto Loader can infer, but inference is non-deterministic.
# If a sensor sends a malformed file, inferred schema changes and breaks downstream.
# Explicit schema = controlled contract between devices and our pipeline.

VITALS_SCHEMA = StructType([
    StructField("device_id",           StringType(),  False),
    StructField("event_timestamp",     StringType(),  False),  # will cast to timestamp
    StructField("ingestion_timestamp", StringType(),  True),
    StructField("heart_rate",          IntegerType(), True),
    StructField("blood_pressure", StructType([
        StructField("systolic",  IntegerType(), True),
        StructField("diastolic", IntegerType(), True),
    ]), True),
    StructField("oxygen_saturation",   IntegerType(), True),
    StructField("patient_type",        StringType(),  True),
    StructField("reading_id",          StringType(),  False),  # dedup key
])

ALARMS_SCHEMA = StructType([
    StructField("device_id",  StringType(), False),
    StructField("timestamp",  StringType(), False),
    StructField("alarm_type", StringType(), False),
    StructField("severity",   StringType(), True),
    StructField("alarm_id",   StringType(), False),
])

# ── Bronze: Vitals ingestion ──────────────────────────────────────────────────
#
# Auto Loader (cloudFiles) vs spark.read:
# - spark.read rescans the entire folder every run → reprocesses all files
# - Auto Loader tracks which files have been processed via checkpoint
# - At scale (50 devices × 30 days = 1,500 files), the difference is significant

vitals_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema/vitals")
        .option("cloudFiles.inferColumnTypes", "false")   # use our explicit schema
        .schema(VITALS_SCHEMA)
        .load(VITALS_LANDING)
)

def process_vitals_batch(batch_df, batch_id):
    """
    Process each micro-batch:
    1. Cast types
    2. Separate valid from invalid (quarantine pattern)
    3. Deduplicate on reading_id (handles sensor double-sends)
    4. MERGE into Bronze table (idempotent)
    """

    # ── Cast timestamps
    batch_df = (batch_df
        .withColumn("event_timestamp",
                    to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_timestamp",
                    to_timestamp(col("ingestion_timestamp")))
        .withColumn("event_date",
                    to_date(col("event_timestamp")))   # partition column
        .withColumn("_ingested_at", current_timestamp())
    )

    # ── Separate valid vs invalid records
    # Invalid = missing primary key or impossible values
    valid_df = batch_df.filter(
        col("device_id").isNotNull() &
        col("reading_id").isNotNull() &
        col("event_timestamp").isNotNull()
    )

    invalid_df = batch_df.filter(
        col("device_id").isNull() |
        col("reading_id").isNull() |
        col("event_timestamp").isNull()
    )

    # ── Quarantine bad records (don't silently drop)
    if invalid_df.count() > 0:
        (invalid_df
            .withColumn("_quarantine_reason", lit("NULL_PRIMARY_KEY_OR_TIMESTAMP"))
            .withColumn("_quarantine_at", current_timestamp())
            .write.format("delta").mode("append").save(QUARANTINE_PATH))
        print(f"⚠️  Batch {batch_id}: {invalid_df.count()} records quarantined")

    # ── Deduplicate within batch (sensor double-sends)
    # Keep one reading per reading_id, prefer the one with ingestion_timestamp
    w = Window.partitionBy("reading_id").orderBy(col("ingestion_timestamp").desc())
    deduped_df = (valid_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── MERGE into Bronze Delta table (idempotent — safe to rerun)
    bronze_table_path = "adb_iot_lakehouse_dev.bronze.vitals"

    if DeltaTable.isDeltaTable(spark, bronze_table_path):
        DeltaTable.forName(spark, bronze_table_path).alias("target").merge(
            deduped_df.alias("source"),
            "target.reading_id = source.reading_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        (deduped_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("event_date")   # partition by date for efficient range queries
            .saveAsTable(bronze_table_path))

    print(f"✅ Batch {batch_id}: {deduped_df.count()} records merged into Bronze")

# ── Start streaming query ─────────────────────────────────────────────────────
vitals_query = (
    vitals_stream.writeStream
        .foreachBatch(process_vitals_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze/vitals")
        .trigger(availableNow=True)   # process all available files then stop
        .start()
)

vitals_query.awaitTermination()
print("✅ Bronze vitals ingestion complete")

# ── Bronze: Alarms (simpler — no dedup needed, alarm_id is unique) ────────────

alarms_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema/alarms")
        .schema(ALARMS_SCHEMA)
        .load(ALARMS_LANDING)
)

def process_alarms_batch(batch_df, batch_id):
    batch_df = (batch_df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("alarm_date", to_date(col("timestamp")))
        .withColumn("_ingested_at", current_timestamp())
        .filter(col("alarm_id").isNotNull())
    )

    alarm_table = "adb_iot_lakehouse_dev.bronze.alarms"

    if DeltaTable.isDeltaTable(spark, alarm_table):
        DeltaTable.forName(spark, alarm_table).alias("t").merge(
            batch_df.alias("s"),
            "t.alarm_id = s.alarm_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta").mode("overwrite") \
            .partitionBy("alarm_date").saveAsTable(alarm_table)

    print(f"✅ Batch {batch_id}: {batch_df.count()} alarms merged")

alarms_query = (
    alarms_stream.writeStream
        .foreachBatch(process_alarms_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze/alarms")
        .trigger(availableNow=True)
        .start()
)

alarms_query.awaitTermination()
print("✅ Bronze alarms ingestion complete")

# ── Validate ──────────────────────────────────────────────────────────────────
display(spark.sql("""
    SELECT
        COUNT(*)                                      AS total_readings,
        COUNT(DISTINCT device_id)                     AS unique_devices,
        COUNT(DISTINCT reading_id)                    AS unique_readings,
        MIN(event_timestamp)                          AS earliest_reading,
        MAX(event_timestamp)                          AS latest_reading,
        COUNT(*) - COUNT(DISTINCT reading_id)         AS duplicate_count
    FROM adb_iot_lakehouse_dev.bronze.vitals
"""))
