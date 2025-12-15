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

## Key Features
**- Multi-class classification:** Distinguishes between 6 traffic types (`Benign`, `DDoS`, `Brute Force`, `BotNet`, `Web Attacks`)

**- Stratified Sampling Strategy:** Ensures balanced representation of each traffic type in the training set

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

### 2. Data Engineering (ETL)
Use a Pyspark job to clean the raw CSVs and apply stratified sampling strategy to ensure balanced representation of each traffic type in the training set.

```bash
python3 data_engineering/etl.py
```

### 3. Model Training
Train a Random Forest model on the cleaned and sampled data.
```bash
python3 model_training/train.py
```

### 4. Run the Simulation (Real-Time Demo)
Run the simulation to generate real-time network traffic data and apply the Random Forest model to detect anomalies.

#### Terminal 1: The Detector (Starts the Engine listening to the Kafka topic)
```bash

```
### Terminal 2: The Producer (Starts the Kafka producer)
```bash

```

## Dashborad Output
Once running, the Spark terminal will display batches of detected traffic in real-time:
```Plaintext
-------------------------------------------
Batch: 15
-------------------------------------------
+----------------+----------------+-------------+------------+
|Predicted_Attack|Destination Port|Flow Duration|Flow Bytes/s|
+----------------+----------------+-------------+------------+
|BENIGN          |443.0           |199.0        |0.0         |
|DDoS            |80.0            |5000000.0    |1500.5      |
|BruteForce      |22.0            |350.0        |4500.2      |
+----------------+----------------+-------------+------------+
```

## Troubleshooting
**1. java.lang.OutOfMemoryError: Java heap space**
- Increase the memory allocated to Docker Desktop.
- Adjust the Spark configuration to use less memory.

**2. Err**

## Future Improvements
- Replace the Random Forest model with a more advanced model. e.g. Pytorch Deep Learning Model, XGBoost, Autoencoder, etc.
- Add Grafana visualization reading from the Spark output sink.
- Deploy to a cloud platform.

