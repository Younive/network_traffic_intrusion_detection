import time
from confluent_kafka import Producer
import json
import csv
import sys

# kafka configuration
KAFKA_TOPIC = 'network_traffic'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'network_traffic_producer',
    'acks': 'all',
    'retries': 5,
    'max.in.flight.requests.per.connection': 1,
    'linger.ms': 10,
    'batch.size': 16384,
    'compression.type': 'snappy'
}

MEASSAGE_PER_SECOND = 100
SLEEP_TIME = 1 / MEASSAGE_PER_SECOND

FILE_PATH = 'data/raw/Friday-WorkingHours-Morning.pcap_ISCX.csv'

# define callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

print('Initializing Kafka producer...')
try: 
    producer = Producer(producer_config)
    print('Producer initialized successfully.')
except Exception as e:
    print(f"An error occurred: {e}")
    sys.exit(1)

def run_producer():
    message_sent = 0
    message_dropped = 0
    start_time = time.time()

    try:
        print(f'Loading data {FILE_PATH}...')
        with open(FILE_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            reader.fieldnames = [field.strip() for field in reader.fieldnames]
            print(f'Loaded {len(reader.fieldnames)}')
            print(f'\nStarting stream to Kafka Topic: {KAFKA_TOPIC}')
            batch_size = 100
            batch_count = 0
            for i, row in enumerate(reader, 1):
                try:
                    raw_row = {key.strip(): value for key, value in row.items()}
                    message_json = json.dumps(raw_row)
                    message_bytes = message_json.encode('utf-8')
                    producer.produce(
                        topic=KAFKA_TOPIC, 
                        value=message_bytes,
                        key=str(i).encode('utf-8'),
                        callback=delivery_report)
                    producer.poll(0)
                    message_sent += 1

                    if message_sent % batch_size == 0:
                        elapsed_time = time.time() - start_time
                        rate = message_sent / elapsed_time if elapsed_time > 0 else 0
                        print(f"Batch {batch_count}: {batch_size} messages sent. Rate: {rate:.2f} msg/s")

                    time.sleep(SLEEP_TIME)

                except Exception as e:
                    print(f"Error processing row {i}: {e}")
                    message_dropped += 1

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except KeyboardInterrupt:
        print("User interrupted the program.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Flushing producer...")
        producer.flush()
        print(f"Total messages sent: {message_sent}")
        print(f"Total messages dropped: {message_dropped}")

if __name__ == "__main__":
    run_producer()