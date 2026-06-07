# 🚗 FleetPulse — Real-Time Vehicle Telemetry & Alerting Pipeline

> A production-style streaming data engineering project built on **Apache Kafka**, **Apache Flink**, and **Confluent Cloud** — simulating IoT telemetry from a 10-vehicle fleet, detecting anomalies in real time, and delivering instant WhatsApp alerts to drivers via Twilio.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Kafka Topics](#kafka-topics)
- [Flink SQL Queries](#flink-sql-queries)
- [Alert Detection Logic](#alert-detection-logic)
- [Avro Schema](#avro-schema)
- [Setup & Running Locally](#setup--running-locally)
- [Environment Variables](#environment-variables)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Future Improvements](#future-improvements)

---

## Overview

FleetPulse is a **hot-path streaming pipeline** that ingests real-time IoT telemetry from a simulated fleet of 10 vehicles, processes it through Apache Flink SQL on Confluent Cloud to detect dangerous conditions, and pushes actionable alerts to drivers via WhatsApp.

**Anomalies detected:**
| Alert Type    | Condition               |
|---------------|-------------------------|
| 🚨 SPEEDING    | `speed_kmph > 80`       |
| ⛽ LOW_FUEL    | `fuel_percent < 15%`    |
| 🌡️ OVERHEATING | `temperature_c > 90°C`  |

---

## Architecture

```
┌──────────────┐     Avro + Schema     ┌──────────────────┐
│  IoT Vehicle │──── Registry ────────▶│  vehicle.         │
│  Simulator   │     (Confluent SR)     │  telemetry-023   │
│  (10 vehicles│                        │  (Kafka Topic)   │
│   Python)    │                        └────────┬─────────┘
└──────────────┘                                 │
                                                 ▼
                                    ┌────────────────────────┐
                                    │     Apache Flink SQL   │
                                    │    (Confluent Cloud)   │
                                    │                        │
                                    │  Detects anomalies:    │
                                    │  • speed > 80 kmph     │
                                    │  • fuel < 15%          │
                                    │  • temp > 90°C         │
                                    └────────────┬───────────┘
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                   ┌──────────────┐   ┌──────────────┐  ┌──────────────────┐
                   │vehicle.speed-│   │vehicle.lowfuel│  │vehicle.overheat- │
                   │ing (Topic)   │   │(Topic)        │  │ing (Topic)       │
                   └──────────────┘   └──────────────┘  └──────────────────┘
                              │                  │                  │
                              └──────────────────┼──────────────────┘
                                                 ▼
                                    ┌────────────────────────┐
                                    │ vehicle.alerts.        │
                                    │ notifications (Topic)  │
                                    │ (unified alert stream) │
                                    └────────────┬───────────┘
                                                 │
                                                 ▼
                                    ┌────────────────────────┐
                                    │   Alert Consumer       │
                                    │   (Python)             │
                                    │                        │
                                    │   Deserializes Avro    │
                                    │   Routes by vehicle_id │
                                    └────────────┬───────────┘
                                                 │
                                                 ▼
                                    ┌────────────────────────┐
                                    │   Twilio WhatsApp API  │
                                    │   → Driver's Phone     │
                                    └────────────────────────┘
```

---

## Tech Stack

| Layer              | Technology                                     |
|--------------------|------------------------------------------------|
| Message Broker     | Apache Kafka (Confluent Cloud)                 |
| Schema Registry    | Confluent Schema Registry (Avro)               |
| Stream Processing  | Apache Flink SQL (Confluent Cloud)             |
| Producer           | Python `confluent-kafka` + `AvroSerializer`    |
| Consumer           | Python `confluent-kafka` + `AvroDeserializer`  |
| Alerting           | Twilio WhatsApp API                            |
| Configuration      | `python-dotenv`                                |

---

## Project Structure

```
fleet-pulse/
│
├── producer/
│   ├── vehicle_simulator.py       # IoT data generator — simulates 10 vehicles
│   └── schemas/
│       └── vehicle_telemetry.avsc # Avro schema for telemetry messages
│
├── consumer/
│   └── consumer_alerts.py         # Kafka consumer for the alerts topic
│
├── alerts/
│   ├── send_alert.py              # Routes alerts to the correct driver
│   └── utilities.py               # Twilio WhatsApp wrapper
│
├── flink/
│   └── flink_queries.sql          # All Flink SQL DDL + INSERT statements
│
├── .env.example                   # Template for required environment variables
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Data Flow

### 1. Producer — `vehicle_simulator.py`

Simulates 10 vehicles (`VEH_001` → `VEH_010`), generating a telemetry payload every 2 seconds per vehicle. Each message is **Avro-serialized** and validated against the schema registered in Confluent Schema Registry before being produced to Kafka.

**Producer features:**
- Idempotent producer (`enable.idempotence=true`) — prevents duplicate messages on retry
- LZ4 compression for efficient network usage
- Delivery callbacks to track success/failure per message
- Linger (`linger.ms=10`) to batch messages for throughput efficiency

**Sample payload:**
```json
{
  "vehicle_id": "VEH_007",
  "timestamp": "2025-12-09T17:36:43.871308+00:00",
  "speed_kmph": 112,
  "fuel_percent": 21,
  "temperature_c": 84,
  "latitude": 40.469962,
  "longitude": -73.359513
}
```

### 2. Stream Processing — Flink SQL

Flink SQL jobs running on Confluent Cloud consume from `vehicle.telemetry-023` and apply filter conditions to route anomalous records into dedicated sink topics and the unified `vehicle.alerts.notifications` topic.

### 3. Consumer — `consumer_alerts.py`

Polls `vehicle.alerts.notifications`, deserializes each Avro message, and passes the alert dict to the alerting layer.

### 4. Alerting — `send_alert.py` + `utilities.py`

Looks up the driver's phone number by `vehicle_id` and sends a formatted WhatsApp message via Twilio.

**Sample alert sent to driver:**
```
⚠️ ALERT: SPEEDING ⚠️
VEHICLE ID: VEH_007
SPEED: 112 KM/HR
FUEL: 21%
TEMPERATURE: 84°C
LOCATION: 40.469962, -73.359513
TIMESTAMP: 2025-12-09T17:36:43.871308+00:00
```

---

## Kafka Topics

| Topic                         | Purpose                                              | Retention |
|-------------------------------|------------------------------------------------------|-----------|
| `vehicle.telemetry-023`       | Raw IoT telemetry from all vehicles                  | Default   |
| `vehicle.speeding`            | Vehicles exceeding 80 kmph                           | Default   |
| `vehicle.lowfuel`             | Vehicles below 15% fuel                              | Default   |
| `vehicle.overheating`         | Vehicles with engine temp above 90°C                 | Default   |
| `vehicle.alerts.notifications`| Unified alert stream (all three anomaly types)       | 1 day     |

---

## Flink SQL Queries

The pipeline uses two patterns:

**Pattern 1 — Dedicated sink per anomaly type** (e.g., `vehicle.speeding`):
```sql
INSERT INTO `vehicle.speeding`
SELECT vehicle_id, CAST(speed_kmph AS INT), 80, `timestamp`, latitude, longitude
FROM `vehicle.telemetry-023`
WHERE speed_kmph > 80;
```

**Pattern 2 — Unified alert topic with CASE-based alert type**:
```sql
INSERT INTO `vehicle.alerts.notifications`
SELECT
  vehicle_id, `timestamp`,
  CASE
    WHEN speed_kmph > 80   THEN 'SPEEDING'
    WHEN fuel_percent < 15  THEN 'LOW_FUEL'
    WHEN temperature_c > 90 THEN 'OVERHEATING'
    ELSE 'UNKNOWN'
  END AS alert_type,
  CAST(speed_kmph AS INT),
  CAST(fuel_percent AS INT),
  CAST(temperature_c AS INT),
  latitude, longitude
FROM `vehicle.telemetry-023`
WHERE speed_kmph > 80 OR fuel_percent < 15 OR temperature_c > 90;
```

> **Note on CAST:** Flink SQL infers `speed_kmph`, `fuel_percent`, and `temperature_c` as `BIGINT` from the Avro source, while the sink tables define them as `INT`. The explicit `CAST` prevents type mismatch errors at runtime.

---

## Alert Detection Logic

| Alert          | Field           | Threshold | Flink Condition       |
|----------------|-----------------|-----------|-----------------------|
| SPEEDING       | `speed_kmph`    | 80 kmph   | `speed_kmph > 80`     |
| LOW_FUEL       | `fuel_percent`  | 15%       | `fuel_percent < 15`   |
| OVERHEATING    | `temperature_c` | 90°C      | `temperature_c > 90`  |

---

## Avro Schema

```json
{
  "type": "record",
  "name": "VehicleTelemetry",
  "namespace": "com.vehicletelemetry",
  "doc": "Vehicle telemetry data for real-time monitoring",
  "fields": [
    { "name": "vehicle_id",    "type": "string", "doc": "Unique vehicle identifier" },
    { "name": "timestamp",     "type": "string", "doc": "ISO-8601 timestamp" },
    { "name": "speed_kmph",    "type": "int",    "doc": "Speed in km/h" },
    { "name": "fuel_percent",  "type": "int",    "doc": "Fuel level 0–100%" },
    { "name": "temperature_c", "type": "int",    "doc": "Engine temperature in °C" },
    { "name": "latitude",      "type": "double", "doc": "GPS latitude" },
    { "name": "longitude",     "type": "double", "doc": "GPS longitude" }
  ]
}
```

Avro was chosen over JSON for its **schema enforcement**, **compact binary serialization**, and native **schema evolution** support via Confluent Schema Registry.

---

## Setup & Running Locally

### Prerequisites

- Python 3.9+
- A [Confluent Cloud](https://confluent.io) account (free tier works)
- A [Twilio](https://twilio.com) account with WhatsApp sandbox enabled
- Confluent Cloud cluster + Schema Registry provisioned
- Apache Flink environment on Confluent Cloud


### 1. Create Kafka topics on Confluent Cloud

Create the following topics in your Confluent Cloud cluster:
- `vehicle.telemetry-023`
- `vehicle.speeding`
- `vehicle.lowfuel`
- `vehicle.overheating`
- `vehicle.alerts.notifications`

[Confluent Cloud](image.png)

### 2. Register the Avro schema

Upload the required Avro Schema `vehicle_telemetry.avsc` to Confluent Schema Registry for the subject `vehicle.telemetry-023-value`.

### 6. Run Flink SQL jobs

Run SQL Queries into the Confluent Cloud Flink SQL workspace and run each statement in order (CREATE TABLE first, then INSERT INTO).

### 7. Start the producer

```bash
vehicle_simulator.py
```

### 8. Start the alert consumer

```bash
consumer_alerts.py
```

You should start seeing telemetry logs in the producer terminal and WhatsApp alerts when anomaly thresholds are crossed.

[Confluent Cloud](image.png)

[Confluent Cloud](image.png)

---

## Environment Variables

| Variable                    | Description                                      |
|-----------------------------|--------------------------------------------------|
| `BOOTSTRAP_SERVERS`         | Confluent Cloud Kafka bootstrap URL              |
| `API_KEY`                   | Confluent Kafka API key                          |
| `API_SECRET`                | Confluent Kafka API secret                       |
| `SCHEMA_REGISTRY_URL`       | Confluent Schema Registry URL                    |
| `SCHEMA_REGISTRY_API_KEY`   | Schema Registry API key                          |
| `SCHEMA_REGISTRY_API_SECRET`| Schema Registry API secret                       |
| `TWILIO_ACCOUNT_SID`        | Twilio Account SID                               |
| `TWILIO_AUTH_TOKEN`         | Twilio Auth Token                                |
| `TWILIO_WHATSAPP_NUMBER`    | Twilio WhatsApp sandbox number                   |

---

## Key Engineering Decisions

**Why Avro + Schema Registry?**
Avro provides compact binary encoding and enforces a contract between producer and consumer. The Schema Registry acts as a central governance layer — if a field type changes, consumers are protected from silent breakage.

**Why idempotent producer?**
Setting `enable.idempotence=true` with `acks=all` guarantees exactly-once delivery at the producer level, preventing duplicate records even during network retries — critical for telemetry pipelines where duplicate alerts would be noisy.

**Why a unified alert topic (`vehicle.alerts.notifications`)?**
Rather than having the consumer subscribe to three separate anomaly topics, a single alerts topic simplifies consumer logic and makes it trivially easy to add new alert types by updating only the Flink INSERT query and the CASE expression.

**Why LZ4 compression?**
LZ4 offers an excellent speed/compression ratio for high-throughput telemetry streams — faster decompression than Snappy with similar compression ratios, and much lower CPU overhead than GZIP.

**1-day retention on alerts topic**
Alerts older than 24 hours are operationally irrelevant, so short retention saves storage while keeping the consumer group lag meaningful.

---

## Future Improvements

- [ ] **Cold path**: Write raw telemetry to an S3/GCS data lake via Kafka Connect for historical analytics
- [ ] **Dashboard**: Real-time Grafana dashboard connected to a TimescaleDB sink
- [ ] **Driver app**: Replace WhatsApp with a dedicated mobile push notification via Firebase FCM
- [ ] **Geofencing alerts**: Flag vehicles that leave a designated operating zone using Flink's geospatial functions

---
# Please Feel Free to 'star' this repository if you found something new.
---

*FleetPulse — because your fleet shouldn't have to wait for a batch job to know it's on fire.*
