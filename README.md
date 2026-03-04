# ICU-Pulse Lakehouse

> **Real-time IoT patient monitoring pipeline — Azure Databricks · Delta Lake · Unity Catalog**

A portfolio project demonstrating production-grade data engineering patterns for healthcare IoT.
50 simulated ICU devices · 14,400 readings/day · medallion architecture · real-time clinical alerting

> ⚠️ All data is synthetically generated. No real patient data is used.

---

## The Problem

I designed this around a hypothetical 50-bed ICU scenario — St. Meridian Medical Center. The constraints of healthcare monitoring create real engineering problems:

| Problem | Engineering Solution |
|---|---|
| Alarm fatigue from false positives | Two-layer anomaly detection (thresholds + rolling window) |
| ~2% duplicate sensor sends (firmware retries) | Deterministic `reading_id` + Delta `MERGE` (idempotent) |
| Late/out-of-order readings over hospital Wi-Fi | 10-minute watermark in Spark Structured Streaming |
| Devices reassigning between patients | SCD Type 2 dimension with temporal joins |
| Patient data access control (HIPAA-adjacent) | Unity Catalog column-level security + purpose-built Gold tables |
| 15-minute alert latency from batch processing | Event Hubs + Structured Streaming, 30-second micro-batch |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    50 ICU DEVICES (simulated)                │
│        Heart Rate · SpO2 · Blood Pressure · every 5 min     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              AZURE EVENT HUBS  (Kafka-compatible)            │
│                    4 partitions · icu-vitals                 │
└──────────────────────────┬──────────────────────────────────┘
                           │  Spark Structured Streaming
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Delta Lake)                │
│  ✓ Auto Loader — incremental, checkpoint-based              │
│  ✓ Explicit schema enforcement (no inference)               │
│  ✓ MERGE on reading_id — deduplication                      │
│  ✓ Quarantine table for malformed payloads                  │
│  ✓ Partitioned by event_date                                │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Delta Lake)                │
│  ✓ Clinical threshold validation (NHS NEWS2)                │
│  ✓ Rolling 10-min average per device (window function)      │
│  ✓ Sudden change detection: >20% deviation = WARNING        │
│  ✓ Composite alert_level: CRITICAL / WARNING / WATCH        │
│  ✓ Incremental MERGE on reading_id                          │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│               GOLD LAYER  (Delta Lake + Z-ORDER)             │
│  ├─ device_daily_metrics   → trend analysis & dashboards    │
│  ├─ clinical_alerts        → nursing station real-time feed  │
│  ├─ ward_summary           → management KPIs                │
│  └─ dim_device_registry    → SCD Type 2 device dimension    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG (Governance)                │
│  ✓ Column-level security on patient_id                      │
│  ✓ Role-based grants: engineers / nurses / analysts         │
│  ✓ Audit logging on all table access                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Clinical Anomaly Detection

This is the core engineering challenge. Standard single-threshold alerting causes alarm fatigue — nurses stop trusting alerts with 80%+ false positive rates.

### Two-Layer Approach

**Layer 1 — Static thresholds (NHS NEWS2)**

Catches absolute clinical danger:

| Vital | Threshold | Alert |
|---|---|---|
| Heart Rate | < 40 bpm | `CRITICAL` — severe bradycardia |
| Heart Rate | > 150 bpm | `CRITICAL` — severe tachycardia |
| SpO2 | < 90% | `CRITICAL` — hypoxia |
| Systolic BP | > 180 mmHg | `CRITICAL` — hypertensive crisis |
| Systolic BP | < 80 mmHg | `CRITICAL` — hypotensive |

*Limitation:* misses gradual deterioration. A patient whose HR rises 70 → 80 → 90 → 100 over 40 minutes never triggers a threshold.

**Layer 2 — Rolling window deviation**

```sql
AVG(heart_rate) OVER (
  PARTITION BY device_id
  ORDER BY event_timestamp
  ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING  -- previous 5 readings (~25 min)
) AS rolling_avg_heart_rate
```

If current reading deviates **> 20%** from that rolling average → `WARNING`

> **Why `ROWS BETWEEN` not `RANGE BETWEEN`?**
> Device transmission intervals aren't perfectly uniform due to network jitter. Row-based windows give a consistent 5-reading baseline regardless of timing gaps.

> **Why exclude `CURRENT ROW`?**
> Comparing the current reading *to* its historical average — including it would bias the deviation toward zero.

**Composite alert level:**

| Level | Condition |
|---|---|
| `CRITICAL` | Any absolute threshold breached |
| `WARNING` | No threshold breach but >20% sudden deviation from rolling avg |
| `WATCH` | Slightly elevated/low — not critical |
| `NORMAL` | All vitals within expected range |

---

## Key Technical Decisions

### Why `MERGE` over `INSERT OVERWRITE`?

`MERGE` is idempotent — the pipeline can fail and restart without creating duplicates. `INSERT OVERWRITE` can't handle late-arriving records without full reprocessing. Trade-off: `MERGE` is ~4x slower, mitigated by including `event_date` in the merge key so Delta only reads today's partition.

### Why partition by `event_date` not `ingestion_date`?

Clinical queries ask *"show me vitals from January 5th"* — that's when the vital was **measured**, not when we received it. Partitioning on the query dimension means those queries read exactly one partition.

### Why Event Hubs over IoT Hub?

This use case is unidirectional — devices push vitals, nothing sends commands back. Event Hubs is Kafka-compatible, integrates natively with Spark Structured Streaming, and is cheaper for pure ingest. IoT Hub adds device management complexity (twin state, C2D messaging, firmware updates) that isn't needed here.

### Why three Gold tables instead of one?

Different consumers, different security requirements:
- **Nursing station** — needs patient-level alerts, sub-second response, `patient_id` included
- **Analysts** — need aggregate device metrics, no patient identity
- **Management** — need daily KPIs only

One generic table would slow real-time queries and make access control messy. Purpose-built tables mean analysts structurally cannot see patient data — it's not in their table.

### Why SCD Type 2 for the device registry?

Devices move between patients in a real ICU. Without history, you can't prove which patient owned a given reading after reassignment. SCD Type 2 tracks every assignment with `effective_from` / `effective_to` dates.

Temporal join:
```sql
vitals.event_date BETWEEN dim.effective_from AND dim.effective_to
```

Correctly attributes every historical reading to the right patient — critical for compliance and retrospective clinical analysis.

---

## Project Structure

```
icu-pulse-lakehouse/
├── notebooks/
│   ├── 01_generate_healthcare_iot_data.py    # 50-device simulator with anomaly injection
│   ├── 02_bronze_ingest.py                   # Auto Loader + dedup + quarantine
│   ├── 03_silver_transformations.py          # Clinical validation + rolling window detection
│   ├── 04_gold_aggregations.py               # Three purpose-built Gold tables
│   ├── 05_snowflake_integration.py           # Optional incremental Snowflake sync
│   ├── 06_streaming_ingestion_event_hubs.py  # Event Hubs streaming with watermarking
│   └── 07_device_registry_scd.py             # SCD Type 2 device dimension
├── config/
│   └── clinical_thresholds.yml              # Externalized NEWS2 parameters (tunable)
├── QUICK_START.md                            # Full Azure + Databricks setup guide
├── requirements.txt
└── .gitignore
```

---

## Quick Setup

See [`QUICK_START.md`](QUICK_START.md) for the full Azure + Databricks walkthrough.

```bash
# 1. Create Azure resources
az group create --name rg-icu-pulse --location eastus

# 2. ADLS Gen2 storage + Event Hubs namespace (see QUICK_START.md)

# 3. Unity Catalog setup
CREATE CATALOG IF NOT EXISTS icu_pulse_dev;
CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.bronze;
CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.silver;
CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.gold;

# 4. Run notebooks in order: 01 → 02 → 03 → 04 → 07
```

**Verify it worked:**
```sql
SELECT 'Bronze' as layer, COUNT(*) as records FROM icu_pulse_dev.bronze.vitals
UNION ALL
SELECT 'Silver', COUNT(*) FROM icu_pulse_dev.silver.vitals_cleaned
UNION ALL
SELECT 'Gold',   COUNT(*) FROM icu_pulse_dev.gold.clinical_alerts;
-- Expected: ~14,400 / ~14,400 / ~720
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Azure Databricks · PySpark |
| Storage | ADLS Gen2 · Delta Lake |
| Streaming ingestion | Azure Event Hubs (Kafka) · Spark Structured Streaming |
| Processing | PySpark · SQL window functions · Delta MERGE |
| Governance | Unity Catalog · column-level security |
| Table format | Delta Lake (ACID · MERGE · Z-ORDER · time travel) |
| Optional sink | Snowflake (incremental sync) |

---

## By the Numbers

| Metric | Value |
|---|---|
| Devices simulated | 50 ICU devices |
| Reading frequency | Every 5 minutes |
| Daily readings | 14,400 |
| Duplicate rate handled | ~2% (sensor firmware retries) |
| Streaming watermark | 10 minutes |
| Rolling window | Previous 5 readings (~25 min) |
| Sudden change threshold | > 20% deviation |
| Gold tables | 3 purpose-built |

---

## Disclaimer

Personal portfolio project. All readings are synthetically generated — no real patient data.
Clinical thresholds based on NHS NEWS2 parameters for illustrative purposes only.
Not validated for real clinical use.
