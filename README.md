# Real-Time Event Streaming & Analytics Pipeline

A high-throughput, event-driven analytics pipeline built with Node.js, Apache Kafka (Redpanda), Redis (Pub/Sub & Caching), and WebSockets. Designed for async event ingestion, real-time metrics aggregation, and low-latency dashboard streaming.

##  Architecture Overview

```text
  +--------------------+
  | Event Producer API |  ---> (HTTP POST /events)
  +--------------------+
            |
            v
  +--------------------+
  | Kafka / Redpanda   |  (Topic: 'telemetry-events')
  +--------------------+
            |
            v
  +--------------------+
  | Consumer Workers   |  ---> Aggregates & User Activity
  +--------------------+
            |
            +---> [ Redis ] (In-Memory Cache & Pub/Sub)
            |
            v
  +--------------------+
  | WebSockets Server  |  ---> Real-time Dashboard Clients
  +--------------------+
