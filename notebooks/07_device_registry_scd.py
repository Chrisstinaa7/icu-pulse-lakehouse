# Notebook: 07_device_registry_scd
# Purpose: Model device-to-patient assignments as SCD Type 2 dimension
#
# WHY THIS MATTERS:
# In a real ICU, devices move between patients. A single device_id can be
# assigned to different patients over time. Without tracking this history,
# you can't answer "which patient had elevated heart rate on Tuesday?"
#
# SCD Type 2 tracks every assignment with effective dates:
#   device_001 → Patient A (Jan 1 - Jan 5)
#   device_001 → Patient B (Jan 6 - Jan 10)
#   device_001 → Patient C (Jan 11 - current)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ── Simulated device assignment changes ──────────────────────────────────────
# In production this comes from the hospital EMR/patient management system
# For demo: simulate device reassignments

device_assignments = [
    # Initial assignments (all devices start Jan 1)
    *[{"device_id": f"device_{i:02d}",
       "patient_id": f"PATIENT-{random.randint(1000, 9999)}",
       "ward_location": random.choice(["ICU-A", "ICU-B", "WARD-3", "WARD-5"]),
       "device_model": random.choice(["Philips MX800", "GE Dash 5000", "Mindray BeneView T8"]),
       "assignment_date": "2025-01-01",
       "unassignment_date": None,  # still active
       "assignment_reason": "ADMISSION"}
      for i in range(1, 51)],
    
    # Some reassignments (devices 1-5 were reassigned on Jan 10)
    *[{"device_id": f"device_{i:02d}",
       "patient_id": f"PATIENT-{random.randint(5000, 5999)}",
       "ward_location": random.choice(["ICU-A", "ICU-B"]),
       "device_model": "Philips MX800",  # same device, new patient
       "assignment_date": "2025-01-10",
       "unassignment_date": None,
       "assignment_reason": "PATIENT_TRANSFER"}
      for i in range(1, 6)],
]

import random
assignments_df = spark.createDataFrame(device_assignments)

# ── SCD Type 2 Implementation ─────────────────────────────────────────────────
# Track full history with effective_from / effective_to dates
# is_current flag for latest assignment

def build_scd_dimension(new_assignments_df):
    """
    Build SCD Type 2 device dimension from assignment events.
    
    SCD Type 2 pattern:
    - Each row represents one assignment period
    - effective_from = when assignment started
    - effective_to = when assignment ended (NULL if current)
    - is_current = 1 for the current assignment, 0 for historical
    - surrogate_key = unique ID for each version of the device record
    """
    
    dim_table = "adb_iot_lakehouse_dev.gold.dim_device_registry"
    
    # Add SCD columns
    scd_df = (new_assignments_df
        .withColumn("effective_from", to_date(col("assignment_date")))
        .withColumn("effective_to", 
            when(col("unassignment_date").isNotNull(),
                 to_date(col("unassignment_date")))
            .otherwise(lit("9999-12-31").cast("date")))  # far future = current
        .withColumn("is_current",
            when(col("unassignment_date").isNull(), lit(1))
            .otherwise(lit(0)))
        .withColumn("surrogate_key",
            concat(col("device_id"), lit("_"), 
                   date_format(col("effective_from"), "yyyyMMdd")))
        .withColumn("_scd_updated_at", current_timestamp())
    )
    
    # Historical assignment closure: if device was reassigned, close previous row
    from pyspark.sql.window import Window
    w = Window.partitionBy("device_id").orderBy("effective_from")
    
    scd_with_closure = (scd_df
        .withColumn("next_effective_from",
            lead("effective_from").over(w))
        .withColumn("effective_to",
            when(col("next_effective_from").isNotNull(),
                 date_sub(col("next_effective_from"), 1))
            .otherwise(col("effective_to")))
        .withColumn("is_current",
            when(col("next_effective_from").isNull(), lit(1))
            .otherwise(lit(0)))
        .drop("next_effective_from")
    )
    
    # Write
    if DeltaTable.isDeltaTable(spark, dim_table):
        # In production: MERGE to handle updates properly
        # For demo: overwrite (simpler)
        scd_with_closure.write.format("delta").mode("overwrite").saveAsTable(dim_table)
    else:
        scd_with_closure.write.format("delta").mode("overwrite").saveAsTable(dim_table)
    
    return scd_with_closure

dim_device = build_scd_dimension(assignments_df)

print("✅ Device registry SCD Type 2 dimension created")
display(dim_device.orderBy("device_id", "effective_from"))

# ── Usage Example: Join with vitals ───────────────────────────────────────────
# Now you can answer "which patient had high heart rate on Jan 5?"

vitals = spark.table("adb_iot_lakehouse_dev.silver.vitals_cleaned")
device_dim = spark.table("adb_iot_lakehouse_dev.gold.dim_device_registry")

# Join using temporal logic: event_date between effective_from and effective_to
vitals_enriched = (vitals
    .join(device_dim,
          (vitals.device_id == device_dim.device_id) &
          (vitals.event_date >= device_dim.effective_from) &
          (vitals.event_date <= device_dim.effective_to),
          "left")
    .select(
        vitals["*"],
        device_dim.patient_id,
        device_dim.ward_location,
        device_dim.device_model,
    )
)

display(vitals_enriched.limit(10))

# ── Query Examples ────────────────────────────────────────────────────────────

# 1. Current device assignments (as of today)
display(spark.sql("""
    SELECT device_id, patient_id, ward_location, device_model, effective_from
    FROM adb_iot_lakehouse_dev.gold.dim_device_registry
    WHERE is_current = 1
    ORDER BY device_id
"""))

# 2. Device assignment history for device_01
display(spark.sql("""
    SELECT device_id, patient_id, ward_location,
           effective_from, effective_to, is_current
    FROM adb_iot_lakehouse_dev.gold.dim_device_registry
    WHERE device_id = 'device_01'
    ORDER BY effective_from
"""))

# 3. Which patient was on device_01 on Jan 5?
display(spark.sql("""
    SELECT patient_id, ward_location
    FROM adb_iot_lakehouse_dev.gold.dim_device_registry
    WHERE device_id = 'device_01'
      AND '2025-01-05' BETWEEN effective_from AND effective_to
"""))

# ── Interview talking point ───────────────────────────────────────────────────
"""
WHY SCD TYPE 2 MATTERS FOR HEALTHCARE IoT:

Without it:
  device_01 → Patient A
  [vitals from device_01 on Jan 5]
  device_01 → Patient B  (reassigned Jan 10)
  
  Now if you query historical vitals, you can't tell which patient they belong to.

With SCD Type 2:
  device_01 → Patient A (Jan 1-9)
  device_01 → Patient B (Jan 10-current)
  
  Temporal join: vitals.event_date BETWEEN effective_from AND effective_to
  → Correctly attributes all vitals to the right patient, even historical data.

This is critical for:
- Compliance: patient data must be correctly attributed
- Clinical: retrospective analysis requires accurate patient history
- Auditing: "show me all vitals for Patient A during their ICU stay"
"""
