# Notebook: 06_streaming_ingestion_event_hubs
# Purpose: Production upgrade — stream from Event Hubs instead of batch files
#
# WHY THIS MATTERS:
# - Batch file ingestion = minutes to hours latency
# - Event Hubs streaming = seconds latency
# - For clinical alerts, 30-second latency vs 15-minute latency matters
#
# ARCHITECTURE CHANGE:
#   Before:  Devices → Files in ADLS → Auto Loader (batch)
#   After:   Devices → Event Hubs → Structured Streaming (real-time)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ── Event Hubs Connection ─────────────────────────────────────────────────────
# In production, these come from Databricks Secret Scope
# Event Hubs is Kafka-compatible, so we use the Kafka connector

EVENT_HUB_NAMESPACE      = dbutils.secrets.get("event-hubs", "namespace")
EVENT_HUB_NAME           = "icu-vitals"
EVENT_HUB_ACCESS_KEY     = dbutils.secrets.get("event-hubs", "access-key")
CONSUMER_GROUP           = "$Default"

# Kafka-compatible connection string
CONNECTION_STRING = f"Endpoint=sb://{EVENT_HUB_NAMESPACE}.servicebus.windows.net/;" \
                    f"SharedAccessKeyName=RootManageSharedAccessKey;" \
                    f"SharedAccessKey={EVENT_HUB_ACCESS_KEY};" \
                    f"EntityPath={EVENT_HUB_NAME}"

EH_SASL = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule ' \
          f'required username="$ConnectionString" password="{CONNECTION_STRING}";'

# ── Schema (same as batch version) ────────────────────────────────────────────
VITALS_SCHEMA = StructType([
    StructField("device_id",           StringType(),  False),
    StructField("event_timestamp",     StringType(),  False),
    StructField("ingestion_timestamp", StringType(),  True),
    StructField("heart_rate",          IntegerType(), True),
    StructField("blood_pressure", StructType([
        StructField("systolic",  IntegerType(), True),
        StructField("diastolic", IntegerType(), True),
    ]), True),
    StructField("oxygen_saturation",   IntegerType(), True),
    StructField("patient_type",        StringType(),  True),
    StructField("reading_id",          StringType(),  False),
])

# ── Read from Event Hubs as stream ────────────────────────────────────────────
vitals_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093")
        .option("subscribe", EVENT_HUB_NAME)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", EH_SASL)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .option("failOnDataLoss", "false")   # devices can restart, partitions can change
        .option("startingOffsets", "latest") # only new events (change to "earliest" for backfill)
        .load()
)

# Event Hubs sends messages as binary — extract the JSON payload
vitals_parsed = (vitals_stream
    .select(
        from_json(col("value").cast("string"), VITALS_SCHEMA).alias("data"),
        col("timestamp").alias("eventhub_enqueued_time"),  # when Event Hub received it
        col("offset"),
        col("partition")
    )
    .select("data.*", "eventhub_enqueued_time", "offset", "partition")
)

# ── Add watermarking for late data ────────────────────────────────────────────
# Watermark = "discard data more than X old"
#
# WHY 5 minutes:
# - Sensors send every 5 minutes
# - Hospital wifi can delay transmission by 2-3 minutes in worst case
# - 5-minute watermark captures 95th percentile latency
# - Beyond 5 minutes, reading is too stale for real-time alerting anyway
#
# WHAT IT DOES:
# - Spark waits up to 5 minutes past the max event_timestamp seen
# - After that, older events are dropped (too late)
# - This prevents unbounded state growth in aggregations

vitals_watermarked = (vitals_parsed
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("_stream_ingested_at", current_timestamp())
    .withWatermark("event_timestamp", "5 minutes")
)

# ── Deduplication & write to Bronze ───────────────────────────────────────────
# Same pattern as batch — MERGE on reading_id
# But now it's continuous, not batch

def process_stream_batch(batch_df, batch_id):
    """
    Process each streaming micro-batch.
    Runs every trigger interval (e.g., every 30 seconds).
    """
    
    if batch_df.isEmpty():
        print(f"⏭️  Batch {batch_id}: Empty — no new events")
        return
    
    # Deduplicate within batch
    from pyspark.sql.window import Window
    w = Window.partitionBy("reading_id").orderBy(col("ingestion_timestamp").desc())
    deduped = (batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    
    # MERGE into Bronze
    bronze_table = "adb_iot_lakehouse_dev.bronze.vitals_streaming"
    
    if DeltaTable.isDeltaTable(spark, bronze_table):
        DeltaTable.forName(spark, bronze_table).alias("t").merge(
            deduped.alias("s"),
            "t.reading_id = s.reading_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        deduped.write.format("delta").mode("overwrite") \
            .partitionBy("event_date") \
            .saveAsTable(bronze_table)
    
    print(f"✅ Batch {batch_id}: {deduped.count()} readings merged (latency: "
          f"{(datetime.now() - batch_df.select(max('event_timestamp')).collect()[0][0]).total_seconds():.1f}s)")

# ── Start streaming query ─────────────────────────────────────────────────────
CHECKPOINT_PATH = "abfss://checkpoints@iotlakehousestorage.dfs.core.windows.net/streaming/vitals"

query = (
    vitals_watermarked.writeStream
        .foreachBatch(process_stream_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")  # micro-batch every 30 seconds
        .start()
)

print("🔴 Streaming from Event Hubs started")
print(f"   Trigger: every 30 seconds")
print(f"   Watermark: 5 minutes")
print(f"   Checkpoint: {CHECKPOINT_PATH}")

# ── Monitor streaming progress ────────────────────────────────────────────────
# In production this would go to Azure Monitor / Datadog / Grafana
import time

while query.isActive:
    status = query.status
    recent = query.recentProgress
    
    if recent:
        latest = recent[-1]
        print(f"\n📊 Progress Report:")
        print(f"   Batch: {latest['batchId']}")
        print(f"   Input rows: {latest.get('numInputRows', 0)}")
        print(f"   Processing time: {latest.get('batchDuration', 0)}ms")
        print(f"   Watermark: {latest.get('eventTime', {}).get('watermark', 'N/A')}")
    
    time.sleep(60)  # report every minute

# To stop: query.stop()

# ── Real-time alerts ──────────────────────────────────────────────────────────
# In parallel, you could run a second streaming query that reads from
# Bronze streaming, applies the Silver clinical logic, and writes directly
# to a real-time alerts sink (e.g., Azure SignalR for dashboard push,
# or Event Hubs for downstream alert routing)
