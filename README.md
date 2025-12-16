# Real-Time Network Traffic Anomalies Detection
A scalable, real-time cybersecurity monitoring pipeline capable of detecting anomalies in network traffic (DDoS, Brute Force, BotNet, Web Attacks) using Apache Spark Structured Streaming and machine learning models.

This project processes high-velocity network flow data, classifies traffic using Random Forest model, and alerts on anomalies with low latency.

## Architecture
The system follows a decoupled microservices architecture containerized with Docker.

**1. Data Ingestion (Producer):** A lightweight Kafka producer that ingests network flow data (CICIDS2017 dataset) from a CSV file and publishes it to a Kafka topic.

**2. Message Broker: Kafka** acts as the high-throughput, distributed messaging system for real-time data processing.   

**3. Data Processing (Consumer):** A Spark Structured Streaming consumer that subscribes to the Kafka topic, processes the data in real-time, and applies machine learning models to detect anomalies.

**4. Inference Engine:** A lightweight inference engine that applies machine learning models to detect anomalies in real-time. It uses Random Forest model for classification and alerts on anomalies with low latency.

**5. Alerting:** Malicious traffic is flagged and displayed on a real-time security dashboard

```plaintext
┌─────────────────────────────────────────────────────────────────┐
│                         Data Sources                            │
│                  (CICIDS2017 Network Flows)                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka Producer                             │
│           Streams CSV → JSON → Kafka Topic                      │
│                   (100-1000 msg/sec)                            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Kafka Cluster                         │
│     Topic: network_traffic (4 partitions, 7-day retention)      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────┐
│            Spark Structured Streaming Consumer                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Kafka Source → Read streaming data                    │  │
│  │ 2. Preprocessing → Normalize, cast types, handle nulls   │  │
│  │ 3. ML Pipeline → StringIndexer → VectorAssembler         │  │
│  │ 4. Inference → Random Forest (20 trees, depth=5)         │  │
│  │ 5. Sinks → Console Dashboard                             │  │
│  └──────────────────────────────────────────────────────────┘  │
└──────────────────────────┬─────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Output & Alerts                            │
│  • Real-time Console Dashboard (5-second batches)               │
│  • Parquet Storage (/app/data/detections)                       │
│  • Attack Statistics (1-minute windows)                         |
└─────────────────────────────────────────────────────────────────┘
```

## Key Features
**- Multi-class classification:** Distinguishes between 6 traffic types (`Benign`, `DDoS`, `Brute Force`, `BotNet`, `Web Attacks`)

**- Stratified Sampling Strategy:** Ensures balanced representation of each traffic type in the training set
- **Rare Attacks** (Web Attack, BotNet): 100% retention
- **Common Traffic** (BENIGN, DDoS, DoS): 10% sampling

**- Memory Optimization:** achieves memory efficiency by using a reduced feature space and batch processing.

**- Fault Tolerance:** Ensures high availability and fault tolerance by using checkpointing and state management.

## Tech Stack
**- Language:** Python 3.9

**- Big Data Engine:** Apache Spark (Pyspark), Spark SQL, Spark MLlib

**- Message Broker:** Apache Kafka

**- Containerization:** Docker, Docker Compose

**- Dependency Management:** uv

## Getting Startd
### Prerequisites
- Docker and Docker Compose
- uv installed (or standard pip).

### 1. Setup Environment
Clone the repository and start the infrastructure:
```bash
git clone https://github.com/yourusername/network-intrusion-detection.git
cd network-intrusion-detection

# install python dependencies
uv sync 

# start the infrastructure
docker compose up -d
```

### 2. Download Dataset
Download the CICID2017 dataset and place CSV files in `data/raw/`
```bash
mkdir -p data/raw
# Download from: https://www.unb.ca/cic/datasets/ids-2017.html
# Place files like:
# - Monday-WorkingHours.pcap_ISCX.csv
# - Tuesday-WorkingHours.pcap_ISCX.csv
# - Wednesday-WorkingHours.pcap_ISCX.csv
# ... etc
```

### 3. Install Python Dependencies
Using uv
```bash
uv sync
```

### 4. Start Infrastructure
```bash
# Start all services (Kafka, Zookeeper, Spark, Schema Registry)
docker-compose up -d

# Wait for services to be ready (30 seconds)
sleep 30

# Verify all services are running
docker-compose ps
```
### Expect output:
```plaintext
NAME             STATUS        PORTS
kafka            Up (healthy)  0.0.0.0:9092->9092/tcp
zookeeper        Up (healthy)  0.0.0.0:2181->2181/tcp
spark-master     Up (healthy)  0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker     Up (healthy)  0.0.0.0:8082->8081/tcp
schema-registry  Up (healthy)  0.0.0.0:8081->8081/tcp
control-center   Up            0.0.0.0:9021->9021/tcp
```

### 5. Data Engineering (ETL)
Process raw CSV files into a clean, balanced training dataset:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 4g \
  --executor-memory 3g \
  /app/src/process_data.py
```
#### Output:
- Process data saved to: `data/processed/train_data.parquet`
- Format: Parquet

### 6. Model Training
Train the Random Forest classifier:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 6g \
  --executor-memory 3g \
  /app/src/train_model.py
```
or
```bash
uv run ./src/train_model.py
```

### 7. Run Real-Time Detection
#### Terminal 1: The Detector (Starts the Engine listening to the Kafka topic)
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /app/src/detect_stream.py
```
### Terminal 2: The Producer (Starts the Kafka producer)
```bash
docker exec -it spark-master python /app/src/producer.py
```

## Dashborad Output
Once running, the Spark terminal will display batches of detected traffic in real-time:
```Plaintext
================================================================================
BATCH: 1 | Records: 156 | Timestamp: 2024-12-16 10:23:45
================================================================================
+----------------+----------------+-------------+------------+---------------+
|Predicted_Attack|Destination Port|Flow Duration|Flow Bytes/s|Flow Packets/s |
+----------------+----------------+-------------+------------+---------------+
|BENIGN          |443             |199          |2450.5      |2.5            |
|DDoS            |80              |999999       |500000.0    |100.0          |
|BENIGN          |8080            |1234         |1000.0      |10.0           |
|DoS             |22              |5000         |2000.0      |40.0           |
|WebAttack       |80              |350          |15000.0     |85.0           |
|BENIGN          |443             |875          |3200.0      |15.0           |
|BruteForce      |22              |12000        |500.0       |8.0            |
|BENIGN          |53              |50           |800.0       |20.0           |
+----------------+----------------+-------------+------------+---------------+

Attack Summary:
+----------------+-----+
|Predicted_Attack|count|
+----------------+-----+
|BENIGN          |120  |
|DDoS            |25   |
|DoS             |7    |
|WebAttack       |3    |
|BruteForce      |1    |
+----------------+-----+
================================================================================

```

## Troubleshooting
**1. java.lang.OutOfMemoryError: Java heap space**
- Increase the memory allocated to Docker Desktop.

- Adjust the Spark configuration to use less memory.

**2. No module named 'pyspark**
- Cause: Running with python instead of spark-submit
```bash
# Always use spark-submit for Spark jobs:
docker exec -it spark-master /opt/spark/bin/spark-submit /app/src/script.py

# NOT:
docker exec -it spark-master python /app/src/script.py 
```

**3. Consumer not showing any data**
```bash
# 1. Check Kafka has messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic network_traffic \
  --from-beginning \
  --max-messages 5

# 2. Check producer is running
docker exec spark-master ps aux | grep kafka_producer

# 3. Delete checkpoints and restart
docker exec spark-master rm -rf /app/data/checkpoints/streaming/*

# 4. Check Spark UI for errors
# Open: http://localhost:4040
```

## Project Structure
```plaintext
network-traffic-anomalies-detection/
├── src/
│   ├── config.py                 # Configuration (paths, columns, constants)
│   ├── preprocessing.py          # Data preprocessing module
│   ├── process_data.py           # ETL pipeline (CSV → Parquet)
│   ├── train_model.py            # Model training script
│   ├── kafka_producer.py         # Kafka producer (CSV → Kafka)
|   ├── debug_streaming.py        # Debugging script for detect_stream.py
│   └── detect_stream.py          # Real-time detection consumer
├── data/
│   ├── raw/                      # Raw CICIDS2017 CSV files
│   ├── processed/                # Processed Parquet files
│   │   └── train_data.parquet/
│   ├── detections/               # Real-time detection results
│   └── checkpoints/              # Streaming checkpoints
├── models/
│   └── network_intrusion_detector_model/  # Trained ML model
├── checkpoints/                  # Spark checkpoints
├── docker-compose.yml            # Infrastructure definition
├── Dockerfile.spark              # Custom Spark image with Python
├── requirements.txt              # Python dependencies
├── pyproject.toml                # uv configuration
└── README.md                     # This file
```

## Future Improvements
- Replace the Random Forest model with a more advanced model. e.g. Pytorch Deep Learning Model, XGBoost, Autoencoder, etc.

- Add Grafana visualization reading from the Spark output sink.

- Store real-time predicting results in Database

- Deploy to a cloud platform.