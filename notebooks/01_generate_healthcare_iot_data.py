# Notebook: 01_generate_healthcare_iot_data
# Purpose: Simulate realistic ICU sensor data including clinical anomalies
#
# KEY UPGRADE from basic version:
# - Generates anomalies (the whole point of healthcare monitoring)
# - Realistic clinical ranges per device/patient profile
# - Duplicate readings (sensors double-send ~2% of the time — must handle downstream)
# - Out-of-order timestamps (network delays in hospital wifi)
# - Scale: 50 devices, 288 readings/device/day (every 5 min = realistic ICU cadence)

# ── Imports ──────────────────────────────────────────────────────────────────
import json
import random
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit

# ── Config ────────────────────────────────────────────────────────────────────
NUM_DEVICES        = 50          # realistic ward size
READINGS_PER_DAY   = 288         # every 5 minutes for 24 hours
ANOMALY_RATE       = 0.05        # 5% of readings are clinically abnormal
DUPLICATE_RATE     = 0.02        # 2% duplicate sends (real IoT behaviour)
LATE_READING_RATE  = 0.03        # 3% arrive out of order (network delay)

VITALS_PATH = "abfss://bronze@iotlakehousestorage.dfs.core.windows.net/healthcare_iot/vitals/"
ALARMS_PATH = "abfss://bronze@iotlakehousestorage.dfs.core.windows.net/healthcare_iot/alarms/"

# ── Patient profiles ──────────────────────────────────────────────────────────
# Different patients have different baseline vitals.
# This is why static thresholds alone are insufficient —
# a resting HR of 55 is normal for an athlete, concerning for an elderly patient.

PATIENT_PROFILES = [
    {"type": "elderly",    "hr_range": (55, 85),  "spo2_range": (93, 99), "weight": 0.4},
    {"type": "adult",      "hr_range": (60, 100), "spo2_range": (96, 100),"weight": 0.4},
    {"type": "post_surgery","hr_range": (70, 110), "spo2_range": (94, 99), "weight": 0.2},
]

def pick_profile():
    weights = [p["weight"] for p in PATIENT_PROFILES]
    return random.choices(PATIENT_PROFILES, weights=weights, k=1)[0]

# Assign one profile per device (stable for the run)
device_profiles = {
    f"device_{i:02d}": pick_profile()
    for i in range(1, NUM_DEVICES + 1)
}

# ── Reading generator ─────────────────────────────────────────────────────────

def generate_reading(device_id, base_timestamp, is_anomaly=False):
    profile = device_profiles[device_id]

    if is_anomaly:
        # Inject a clinically significant anomaly
        anomaly_type = random.choice(["tachycardia", "bradycardia", "hypoxia", "hypertension"])
        if anomaly_type == "tachycardia":
            heart_rate = random.randint(151, 200)   # > 150 bpm = clinical threshold
        elif anomaly_type == "bradycardia":
            heart_rate = random.randint(20, 39)     # < 40 bpm = clinical threshold
        else:
            heart_rate = random.randint(*profile["hr_range"])

        oxygen_saturation = random.randint(80, 89) if anomaly_type == "hypoxia" else \
                            random.randint(*profile["spo2_range"])
        systolic  = random.randint(180, 220) if anomaly_type == "hypertension" else \
                    random.randint(110, 130)
        diastolic = random.randint(100, 130) if anomaly_type == "hypertension" else \
                    random.randint(70, 90)
    else:
        heart_rate        = random.randint(*profile["hr_range"])
        oxygen_saturation = random.randint(*profile["spo2_range"])
        systolic          = random.randint(110, 130)
        diastolic         = random.randint(70, 90)

    # Simulate out-of-order arrival (late network delivery)
    arrival_offset = timedelta(minutes=random.randint(2, 15)) \
                     if random.random() < LATE_READING_RATE else timedelta(0)

    return {
        "device_id":          device_id,
        "event_timestamp":    base_timestamp.isoformat(),          # when sensor measured
        "ingestion_timestamp":(base_timestamp + arrival_offset).isoformat(),  # when received
        "heart_rate":         heart_rate,
        "blood_pressure": {
            "systolic":  systolic,
            "diastolic": diastolic
        },
        "oxygen_saturation":  oxygen_saturation,
        "patient_type":       device_profiles[device_id]["type"],  # for context
        "reading_id":         f"{device_id}_{int(base_timestamp.timestamp())}",  # dedup key
    }

# ── Generate data ─────────────────────────────────────────────────────────────

vitals_list = []
base_time   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

for device_id in device_profiles:
    for i in range(READINGS_PER_DAY):
        ts        = base_time + timedelta(minutes=i * 5)
        is_anomaly = random.random() < ANOMALY_RATE
        reading   = generate_reading(device_id, ts, is_anomaly)
        vitals_list.append(reading)

        # Simulate duplicate send (sensor firmware bug / retry logic)
        if random.random() < DUPLICATE_RATE:
            vitals_list.append(reading.copy())  # exact duplicate

print(f"Generated {len(vitals_list)} readings "
      f"({READINGS_PER_DAY * NUM_DEVICES} expected + ~{int(DUPLICATE_RATE * READINGS_PER_DAY * NUM_DEVICES)} duplicates)")

# ── Write to ADLS ─────────────────────────────────────────────────────────────
# Partitioned by device_id so Auto Loader / downstream reads are efficient

vitals_df = spark.createDataFrame(vitals_list)

vitals_df.write \
    .mode("append") \
    .partitionBy("device_id") \
    .json(VITALS_PATH)

print(f"✅ Vitals written to {VITALS_PATH}")
print(f"   Partitioned by device_id for efficient downstream reads")

# ── Generate alarm events (separate stream) ───────────────────────────────────
# Alarms are a separate event type — device fires these independently of readings

ALARM_TYPES = [
    "HIGH_HEART_RATE", "LOW_HEART_RATE",
    "LOW_OXYGEN", "HIGH_BP",
    "SENSOR_DISCONNECT", "BATTERY_LOW"
]

alarms_list = []
for device_id in device_profiles:
    # 0-3 alarms per device per day
    for _ in range(random.randint(0, 3)):
        ts = base_time + timedelta(minutes=random.randint(0, 1439))
        alarms_list.append({
            "device_id":   device_id,
            "timestamp":   ts.isoformat(),
            "alarm_type":  random.choice(ALARM_TYPES),
            "severity":    random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
            "alarm_id":    f"ALM_{device_id}_{int(ts.timestamp())}",
        })

alarms_df = spark.createDataFrame(alarms_list)
alarms_df.write.mode("append").partitionBy("device_id").json(ALARMS_PATH)

print(f"✅ {len(alarms_list)} alarm events written to {ALARMS_PATH}")
