# Distributed Real-Time Financial Asset Predictor

## Architecture Overview
This project is an end-to-end, real-time quantitative trading pipeline built on **Apache Spark**. It is designed to ingest live market tick data, engineer financial features on the edge, and execute low-latency machine learning predictions across a distributed computing cluster.

To bypass local network constraints and simulate a multi-region deployment, the cluster utilizes a **Tailscale Mesh VPN**, allowing Spark Master and Worker nodes to communicate securely over the internet without complex port-forwarding.

## System Components
1. **Phase 0: Offline ML Engine (`train_model.py`)**
   - Ingests historical Yahoo Finance data.
   - Engineers quantitative features: 5-Day Moving Average (`MA_5`) and 5-Day Volatility (`Volatility_5`).
   - Trains and serializes a native PySpark `RandomForestClassifier` PipelineModel.
2. **Phase 1: Edge Processing Producer (`producer.py`)**
   - Python-based TCP socket server bound to the Tailscale VPN interface.
   - Fetches live market data and calculates real-time rolling metrics on the edge.
   - Broadcasts the finalized Data Contract string: `Timestamp, Ticker, Close, Volume, MA_5, Volatility_5`.
3. **Phase 2: Distributed ML Consumer (`consumer.py`)**
   - PySpark Structured Streaming application.
   - Ingests the micro-batch TCP socket stream.
   - Loads the serialized Random Forest model for in-memory, real-time inference (BUY / HOLD/SELL).

## Technology Stack
* **Distributed Computing:** Apache Spark (PySpark), Spark Structured Streaming
* **Networking:** TCP Sockets, Tailscale (Zero-Config WireGuard VPN)
* **Machine Learning:** PySpark MLlib (Random Forest Regressor)
* **Data Sources:** Yahoo Finance API (`yfinance`)

## Execution
*The system requires a Tailscale IP to bind the Spark daemons to the VPN mesh.*
1. Start Spark Master: `start-master.sh -h <TAILSCALE_IP>`
2. Start Spark Worker: `start-worker.sh spark://<TAILSCALE_IP>:7077`
3. Launch Producer: `python producer.py`
4. Launch Streaming Engine: `python consumer.py`
