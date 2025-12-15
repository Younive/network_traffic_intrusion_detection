from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel
from preprocessing import get_preprocessor
from config import MODEL_PATH, COLUMNS
import sys

# create spark session
spark = SparkSession.builder \
    .appName('NetworkIntrusionDetector') \
    .master('spark://spark-master:7077') \
    .config('spark.driver.memory', '2g') \
    .config('spark.sql.streaming.checkpointLocation', '/app/data/checkpoints/streaming') \
    .getOrCreate()

KAFKA_BOOTSTRAP_SERVERS = 'kafka:29029'
KAFKA_TOPIC = 'network_traffic'
CHECKPOINT_DIR = 'app/data/checkpoint/streaming'

FEATURE_COLS = [c for c in COLUMNS if c != 'Label']

# load model
print('\nLoading Model...')
try:
    model = PipelineModel.load(MODEL_PATH)
except Exception as e:
    print(f"Error loading model: {e}")
    spark.stop()
    sys.exit(1)

# initialze data preprocessor
preprocessor = get_preprocessor(feature_cols=FEATURE_COLS)

# connect to kafka
print('\nConnecting to Kafka...')
try:
    raw_stream = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
        .option('subscribe', KAFKA_TOPIC) \
        .option('startingOffsets', 'latest') \
        .option('failOnDataLoss', 'false') \
        .option('maxOffsetPerTringger', '1000') \
        .load()
    print('Connected to Kafka successfully.')
except Exception as e:
    print(f'Error connecting to Kafka: {e}')
    spark.stop()
    sys.exit(1)

# create json schema
print('\nDefining JSON schema...')
json_schema = StructType([
    StructField(col_name, DoubleType(), True) for col_name in FEATURE_COLS
])
print(f'Schema defined for {len(FEATURE_COLS)} features')

# parse kafka value as json
print('\nSetting up message parsing pipeline...')
data_stream = raw_stream.selectExpr('CAST(key AS STRING) as message_key',
                'CAST(value AS STRING) as json_string',
                'timestamp as kafka_timestamp',
                'topic',
                'partition',
                'offset') \
    .select(
        col('message_key'),
        col('kafka_timestamp'),
        col('topic'),
        col('offset'),
        from_json(col('json_string'), json_schema).alias('data')) \
    .select(
        col('message_key'),
        col('kafka_timestamp'),
        col('topic'),
        col('partition'),
        col('offset'),
        'data.*'
    )
data_stream = data_stream.na.fill(0.0)

print('Parsing pipeline configured.')

# apply data preprocessing
data_stream_clean = preprocessor.preprocess_for_inference(data_stream)
data_steam_clean = data_steam_clean.withColumn('processing_time', current_timestamp())
print('Preprocessing pipeline ready.')

# apply model
print('\nSetting up model inference...')
try:
    detections_stream = model.transform(data_stream_clean)
except Exception as e:
    print(f'Error applying model: {e}')
    spark.stop()
    sys.exit(1)

# map prediction
print('\nConfiguring prediction mapping...')
LABELS = ['BENIGN', 'DoS', 'WebAttack', 'BruteForce', 'BotNet', 'DDoS']

def map_prediction(prediction_idx):
    try:
        if prediction_idx is None:
            return 'Unknown'
        idx = int(prediction_idx)
        if 0 <= idx < len(LABELS):
            return LABELS[idx]
        else:
            return 'Unknown'
    except Exception as e:
        print(f'Error mapping prediction: {e}')
        return 'Unknown'

# register UDF
map_prediction_udf = udf(map_prediction, StringType())

# add predicted attack label
final_stream = detections_stream.withColumn(
    'Predicted_Attack', 
    map_prediction_udf(col('prediction'))
)
print(f'Prediction mapping configured. ({len(LABELS)} labels)')

# select columns for console output
output_columns = [
    'kafka_timestamp',
    'processing_time',
    'Predicted_Attack',
    'prediction'
]
key_features = [
    'Destination Port',
    'Flow Duration',
    'Flow Bytes/s',
    'Flow Packets/s',
    'Total Fwd Packets',
    'Total Backward Packets'
]

for feature in key_features:
    if feature in final_stream.columns:
        output_columns.append(feature)

output_steam = final_stream.select(*output_columns)

print(f'Output Configured. Output columns: {output_columns}')

# output sink 1: console
print('\nStarting Console Output...')
console_query = output_steam.writeStream \
    .format('console') \
    .outputMode('append') \
    .option('truncate', False) \
    .option('numRows', 20) \
    .trigger(processingTime='5 seconds') \
    .option('checkpointLocation', f'{CHECKPOINT_DIR}/console') \
    .start()
print('Console output started.')


print('Dashboard is live. Waiting for traffic...')

# wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print('\nStopping Spark session...')
    spark.stop()
    print('Spark session stopped.')

# stop all queries
queries = [console_query]
for query in queries:
    query.stop()
    print(f'Stopped {query.name}')

print('All queries stopped.')
spark.stop()
print('Spark session stopped.')