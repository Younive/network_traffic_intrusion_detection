"""
debug_streaming.py - Debug why no data appears in console
Run this to diagnose the issue
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count as spark_count
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import time

# ============================================================================
# STEP 1: CHECK KAFKA CONNECTION
# ============================================================================
print('\n' + '='*80)
print('STEP 1: Checking Kafka Connection')
print('='*80)

KAFKA_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'network_traffic'

spark = SparkSession.builder \
    .appName('Debug') \
    .master('local[2]') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

try:
    # Try to read from Kafka
    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_SERVERS) \
        .option('subscribe', KAFKA_TOPIC) \
        .option('startingOffsets', 'earliest') \
        .load()
    
    print('✅ Can connect to Kafka')
    print(f'✅ Topic "{KAFKA_TOPIC}" is accessible')
    
except Exception as e:
    print(f'❌ Cannot connect to Kafka: {e}')
    print('\nTroubleshooting:')
    print('1. Check Kafka is running: docker ps | grep kafka')
    print('2. Check topic exists: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092')
    spark.stop()
    exit(1)

# ============================================================================
# STEP 2: CHECK IF TOPIC HAS MESSAGES
# ============================================================================
print('\n' + '='*80)
print('STEP 2: Checking if Topic Has Messages')
print('='*80)

def check_messages(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f'✅ Batch {batch_id}: Found {count} messages in Kafka')
        print('\nFirst message:')
        batch_df.select('value').show(1, truncate=False)
    else:
        print(f'⚠️  Batch {batch_id}: No messages found')
        print('\nPossible reasons:')
        print('1. Producer is not running')
        print('2. Producer sent to wrong topic')
        print('3. Messages already consumed (startingOffsets=latest)')

query = df.writeStream \
    .foreachBatch(check_messages) \
    .trigger(processingTime='5 seconds') \
    .start()

print('\nWaiting for messages (10 seconds)...')
time.sleep(10)
query.stop()

# ============================================================================
# STEP 3: CHECK MESSAGE FORMAT
# ============================================================================
print('\n' + '='*80)
print('STEP 3: Checking Message Format')
print('='*80)

df_parsed = df.selectExpr('CAST(value AS STRING) as json_string')

def check_format(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f'✅ Batch {batch_id}: Can parse {count} messages as strings')
        print('\nSample message content:')
        batch_df.show(1, truncate=100)
        
        # Try to parse as JSON
        sample = batch_df.take(1)[0]['json_string']
        print(f'\nRaw JSON (first 200 chars):')
        print(sample[:200])
        
        # Check if it's valid JSON
        import json
        try:
            parsed = json.loads(sample)
            print(f'\n✅ Valid JSON with {len(parsed)} fields')
            print(f'Sample fields: {list(parsed.keys())[:5]}')
        except Exception as e:
            print(f'\n❌ Invalid JSON: {e}')
    else:
        print(f'⚠️  No messages to parse')

query2 = df_parsed.writeStream \
    .foreachBatch(check_format) \
    .trigger(processingTime='5 seconds') \
    .start()

print('\nWaiting for messages (10 seconds)...')
time.sleep(10)
query2.stop()

# ============================================================================
# STEP 4: TEST COMPLETE PIPELINE
# ============================================================================
print('\n' + '='*80)
print('STEP 4: Testing Complete Pipeline')
print('='*80)

# Simple schema for testing
# test_schema = StructType([
#     StructField('Destination Port', DoubleType(), True),
#     StructField('Flow Duration', DoubleType(), True),
#     StructField('Flow Bytes/s', DoubleType(), True),
# ])

# df_final = df \
#     .selectExpr('CAST(value AS STRING) as json_string') \
#     .select(from_json(col('json_string'), test_schema).alias('data')) \
#     .select('data.*') \
#     .na.fill(0.0)

# try to fix schema issue
test_schema = StructType([
    StructField('Destination Port', StringType(), True),
    StructField('Flow Duration', StringType(), True),
    StructField('Flow Bytes/s', StringType(), True),
])

# cast to double
df_final = df \
    .selectExpr('CAST(value AS STRING) as json_string') \
    .select(from_json(col('json_string'), test_schema).alias('data')) \
    .select('data.*') \
    .select(
        col('Destination Port').cast('double').alias('Destination Port'),
        col('Flow Duration').cast('double').alias('Flow Duration'),
        col('Flow Bytes/s').cast('double').alias('Flow Bytes/s')
    )
df_final = df_final.na.fill(0.0)

def check_pipeline(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f'✅ Batch {batch_id}: Pipeline working! {count} records processed')
        print('\nSample data:')
        batch_df.show(5, truncate=False)
    else:
        print(f'⚠️  Batch {batch_id}: Pipeline not receiving data')

query3 = df_final.writeStream \
    .foreachBatch(check_pipeline) \
    .trigger(processingTime='5 seconds') \
    .start()

print('\nWaiting for messages (10 seconds)...')
time.sleep(10)
query3.stop()
spark.stop()