# HVAC Telemetry Lakehouse: Kafka to DuckDB

A proof of concept that measures where the time actually goes in a small data platform: from a telemetry event entering Kafka, through a Delta Lake medallion pipeline, to an analytical query rendered in a browser.

The domain is industrial HVAC telemetry for chillers: temperatures, flow rates, compressor amps, fault codes, power draw and cost. The data is simulated, the streaming path is real.

The point of the repo is the timing. Every stage writes its own duration to `pipeline_execution.txt`, the API returns `latency_ms` on every response, and the UI shows it. Nothing here claims a number it did not measure.

## Architecture

```text
                    ┌───────────────────────────────────────────┐
                    │  Aiven Kafka (TLS)  topic: hvac_telemetry │
                    └───────────────┬───────────────────────────┘
                                    │ keyed by Device_ID
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
    realtime_producer.py    kafka_to_batch.py    realtime_backend/
      produces events       drains the topic     consumer thread
              │                     │                     │
              │                     ▼                     ▼
              │            real_time_data.json      in-memory buffer (100)
              │                     │                     │
              │                     │                     ▼
              │                     │              SSE /api/stream
              │                     │                     │
              │                     │                     ▼
              │                     │            browser live dashboard
              ▼                     │
    ┌───────────────────┐           │
    │ generator (dirty) │           │
    │ 100,000 records   │           │
    └─────────┬─────────┘           │
              ▼                     │
        data_lake/batch  ◄──────────┘
              │
              ▼
        data_lake/staging      dropna + dropDuplicates
              │
              ▼
        data_lake/silver       18 type casts + 16 business columns = 56
              │
              ▼
        data_lake/gold         4 dimension tables + 11 fact and mart tables
              │                 Z-Ordering on the tables the API filters
              ▼
     ┌────────────────┐        Hugging Face bucket (object store)
     │ backend/main.py│ ──────► upload + download via sync_bucket.py
     │ FastAPI+DuckDB │
     └────────┬───────┘
              │ zero-copy: DeltaTable → PyArrow dataset → con.register()
              ▼
     ┌────────────────┐
     │ frontend/       │  Next.js explorer + raw SQL console
     └────────────────┘
```

## The streaming path is real

Three separate Kafka components, each with a different job:

| File | Role | Consumer group | Offset policy |
|---|---|---|---|
| `realtime_producer.py` | Publishes telemetry every 5 seconds, keyed by `Device_ID` | n/a | n/a |
| `realtime_backend/main.py` | Background consumer thread, feeds the SSE dashboard | `realtime_dashboard_group` | `latest` |
| `kafka_to_batch.py` | Drains the topic to JSONL for the batch pipeline | `github_actions_batch_group` | `earliest` |

The producer uses `confluent_kafka.Producer` over TLS with a delivery-report callback, so a failed publish is logged rather than silently dropped. Keying by device ID keeps one device's readings on one partition.

The two consumers deliberately use different groups and different offset policies. The dashboard wants the newest readings, so it starts at `latest`. The batch pipeline wants the whole topic, so it starts at `earliest` and drains until three consecutive polls come back empty.

## Every component has a fallback

If `KAFKA_BOOTSTRAP_SERVERS` is unset, the producer runs in dry-run mode, the SSE server generates mock readings, and the batch drain falls back to the file generator. The pipeline still runs end to end.

This was a deliberate design decision, not an accident. A demo that dies when a cloud credential expires cannot be run in CI, and the batch pipeline is the thing that has to work on every run.

## The medallion layers

**Staging** drops records missing a device ID or either core temperature sensor, and removes exact duplicates.

**Silver** casts 18 string columns to their real types, then derives 16 business columns:

- `Setpoint_Deviation` and `SLA_Breach_Flag`, where a breach is a deviation above 2.0 degrees
- `Estimated_Hourly_Cost_USD`, derived from power draw at 0.12 per kWh
- `Air_Quality_Status` from CO2 levels, `Maintenance_Recommendation` from filter pressure drop, `Asset_Health_Status` from the fault code
- `Cooling_Capacity_Delta` and the evaporator and condenser approach temperatures

**Gold** publishes 15 tables: `dim_device`, `dim_time`, `dim_fault_codes`, `dim_status_flags`, plus `fact_hourly_telemetry`, `fact_fault_events` and nine business marts covering daily cost, shift performance, SLA breaches, compressor health, environmental compliance, pump maintenance, cooling efficiency, valve operations and filter diagnostics.

**Z-Ordering** runs on the ten fact and mart tables the read path filters on, clustered on the same columns the predicates use: `Device_ID` and `Date` for the telemetry and device-scoped marts, `Date` and `Operational_Shift` for the shift-scoped one. Delta then skips Parquet files whose range cannot match, so a filtered query touches fewer files. At this data volume the tables are small enough to land in one file each, so the compaction is mostly a no-op here. The pattern is what carries to a table with real history.

## The dirty data is the point

`pipelines/generator_and_upload.py` does not produce clean records. Every value is written as a string, about 8 percent of rows get a null entering temperature, another 6 percent get a null leaving temperature, and roughly 5 percent carry a fault code. The setpoint is written as `40.00001525878906`, a float artefact that makes the SLA threshold meaningful instead of round.

Without that, the casting and the null handling in Silver would be untested.

## The serving path

`backend/main.py` reads the gold Delta tables, converts them to PyArrow datasets, registers them in DuckDB and serves them over FastAPI. No data is copied into DuckDB. The frontend has two views: a paginated table explorer over each layer and a SQL console that runs raw SQL against the gold tables with a forced `LIMIT`.

Every response carries `latency_ms`, and the UI shows it next to the result.

## Setup

```bash
# Batch pipeline
pip install -r requirements.txt
export HF_TOKEN=...
python main.py

# Analytical API
pip install -r backend/requirements.txt
cd backend && uvicorn main:app --port 8000

# Live streaming API
pip install -r realtime_backend/requirements.txt
cd realtime_backend && uvicorn main:app --port 8001

# Front end
cd frontend && npm install && npm run dev
```

Download the generated lake from the Hugging Face bucket with `python sync_bucket.py`, then point the API at `./data_lake`.

### Environment variables

Copy `.env.example` to `.env` and fill in what you need. Only `KAFKA_BOOTSTRAP_SERVERS` and `HF_TOKEN` are required, and both have working fallbacks.

The Aiven service credentials (`ca.pem`, `service.cert`, `service.key`) go in the repository root. They are gitignored.

## CI

`.github/workflows/pipeline.yml` runs the whole medallion pipeline on GitHub Actions with `workflow_dispatch`. It needs the `HF_TOKEN` secret and nothing else, because the Kafka path falls back to generated data.

## What is real and what is not

Real: the Kafka producer and both consumers, the Delta Lake medallion pipeline with Z-Ordering, the DuckDB serving path, the latency instrumentation, the CI workflow.

Simulated: the telemetry itself. The values are random draws with deliberate defects, and the cost model is a flat 0.12 per kWh. The schema, the layer contracts and the failure handling are the parts worth reading.
