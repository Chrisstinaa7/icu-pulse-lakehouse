# Quick Start — ICU-Pulse Lakehouse

Get the full pipeline running in ~30 minutes.

---

## Prerequisites

- Azure subscription with contributor access
- Databricks workspace (Premium tier for Unity Catalog)
- Python 3.9+
- Azure CLI (optional but recommended)

---

## Step 1: Azure Resources

### Resource Group
```bash
az group create --name rg-icu-pulse --location eastus
```

### ADLS Gen2 Storage
```bash
az storage account create \
  --name icupulsestorage \
  --resource-group rg-icu-pulse \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create containers
for container in bronze silver gold checkpoints quarantine; do
  az storage container create \
    --name $container \
    --account-name icupulsestorage
done
```

### Event Hubs
```bash
az eventhubs namespace create \
  --name icu-pulse-events \
  --resource-group rg-icu-pulse \
  --sku Standard

az eventhubs eventhub create \
  --name icu-vitals \
  --namespace-name icu-pulse-events \
  --resource-group rg-icu-pulse \
  --partition-count 4
```

---

## Step 2: Databricks Setup

1. Create workspace via Azure Portal → Azure Databricks → Premium tier
2. Create cluster: Runtime 13.3 LTS, Standard_DS3_v2, 2-4 workers
3. Install cluster libraries: `azure-eventhub==5.11.4`

### Unity Catalog
```sql
CREATE CATALOG IF NOT EXISTS icu_pulse_dev;

CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.bronze;
CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.silver;
CREATE SCHEMA IF NOT EXISTS icu_pulse_dev.gold;
```

### Permissions
```sql
-- Engineers
GRANT ALL PRIVILEGES ON SCHEMA icu_pulse_dev.bronze TO `data-engineers`;

-- Analysts (read Gold only, no patient_id)
GRANT SELECT ON icu_pulse_dev.gold.device_daily_metrics TO `analysts`;
GRANT SELECT ON icu_pulse_dev.gold.ward_summary TO `analysts`;

-- Nurses (read clinical alerts including patient_id)
GRANT SELECT ON icu_pulse_dev.gold.clinical_alerts TO `icu-nurses`;
```

---

## Step 3: Secrets Configuration

```bash
# Storage access
databricks secrets create-scope --scope azure-storage
databricks secrets put --scope azure-storage --key storage-key

# Snowflake (optional, for notebook 05)
databricks secrets create-scope --scope snowflake-scope
databricks secrets put --scope snowflake-scope --key sf-url
databricks secrets put --scope snowflake-scope --key sf-user
databricks secrets put --scope snowflake-scope --key sf-password

# Event Hubs (for notebook 06)
databricks secrets create-scope --scope event-hubs
databricks secrets put --scope event-hubs --key namespace
databricks secrets put --scope event-hubs --key access-key
```

---

## Step 4: Run the Pipeline

Run notebooks in order:

```
01_generate_healthcare_iot_data.py   → generates 14,400 synthetic vitals + alarms
02_bronze_ingest.py                  → Auto Loader ingestion + deduplication
03_silver_transformations.py         → clinical validation + anomaly detection
04_gold_aggregations.py              → three Gold tables
07_device_registry_scd.py            → SCD Type 2 device dimension (can run anytime)
```

**Optional:**
```
05_snowflake_integration.py          → sync Gold to Snowflake
06_streaming_ingestion_event_hubs.py → real-time Event Hubs ingestion
```

---

## Step 5: Verify

```sql
-- Data at each layer
SELECT 'Bronze' as layer, COUNT(*) as records FROM icu_pulse_dev.bronze.vitals
UNION ALL
SELECT 'Silver', COUNT(*) FROM icu_pulse_dev.silver.vitals_cleaned
UNION ALL
SELECT 'Gold - Alerts', COUNT(*) FROM icu_pulse_dev.gold.clinical_alerts;

-- Expected: ~14,400 Bronze, ~14,400 Silver, ~720 CRITICAL+WARNING alerts

-- Critical alerts (should be ~5% of readings)
SELECT alert_level, COUNT(*) as count
FROM icu_pulse_dev.silver.vitals_cleaned
GROUP BY alert_level
ORDER BY count DESC;

-- Device health scores
SELECT device_id, device_health_score, critical_alerts
FROM icu_pulse_dev.gold.device_daily_metrics
ORDER BY device_health_score ASC
LIMIT 10;
```

---

## Troubleshooting

**Permission denied on ADLS:**
```bash
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <service-principal-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-icu-pulse
```

**Table not found:**
```sql
SHOW SCHEMAS IN icu_pulse_dev;
-- Make sure schemas exist before running ingestion
```

**Event Hubs timeout (notebook 06):**
Check that `icu-vitals` event hub exists and connection string is correctly stored in secret scope.

---

## Cost Estimate (Development)

| Resource | Daily Cost |
|---|---|
| ADLS Gen2 (100 GB) | ~$1 |
| Event Hubs Standard | ~$2 |
| Databricks (8 hrs/day) | ~$10 |
| **Total** | **~$13/day** |

Use Databricks Jobs with auto-termination to minimize compute costs.
