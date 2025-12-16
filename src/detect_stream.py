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

KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
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
        .option('maxOffsetsPerTrigger', '1000') \
        .load()
    print('Connected to Kafka successfully.')
except Exception as e:
    print(f'Error connecting to Kafka: {e}')
    spark.stop()
    sys.exit(1)

# create json schema
print('\nDefining JSON schema...')
json_schema = StructType([
    StructField(col_name, StringType(), True) for col_name in FEATURE_COLS
])
print(f'Schema defined for {len(FEATURE_COLS)} features')

# parse kafka message
print('\nSetting up message parsing pipeline...')
data_stream = raw_stream \
    .selectExpr(
        'CAST(value AS STRING) as json_string',
        'timestamp as kafka_timestamp') \
    .select(
        col('kafka_timestamp'),
        from_json(col('json_string'), json_schema).alias('data')) \
    .select(
        col('kafka_timestamp'),
        'data.*'
    )
print('Casting string values to doubles...')
cast_exprs = [col('kafka_timestamp').cast('timestamp')]
for feature in FEATURE_COLS:
    cast_exprs.append(
        when(col(feature).isNull(), 0.0)
        .otherwise(col(feature).cast(DoubleType()))
        .alias(feature)
    )
data_stream = data_stream.select(*cast_exprs)
data_stream = data_stream.na.fill(0.0)
data_stream = data_stream.withColumn('processing_time', current_timestamp())
print('Parsing pipeline configured.')

# apply data preprocessing
data_stream_clean = preprocessor.preprocess_for_inference(data_stream)
print('Preprocessing pipeline ready.')

# apply model
print('\nSetting up model inference...')
try:
    detections_stream = model.transform(data_stream_clean)
    print('Model inference ready\n.')
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
dashboard_column = ['Predicted_Attack']
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
        dashboard_column.append(feature)

output_steam = final_stream.select(*dashboard_column)

# custom function for console output
batch_count = [0]
def print_dashboard(batch_df, batch_id):
    batch_count[0] += 1

    # get row count
    count = batch_df.count()
    if count > 0:
        # Clear screen effect
        print('\n' * 2)
        print('='*80)
        print(f'BATCH: {batch_count[0]} | Records: {count} | Timestamp: {current_timestamp()}')
        print('='*80)
        # show data
        batch_df.show(
            n=20, # show 20 rows
            truncate=False,
            vertical=False
        )

        print('\n Attack Summary:')
        attack_summary = batch_df.groupBy('Predicted_Attack').count().orderBy('count', ascending=False)
        attack_summary.show(truncate=False)
        print('='*80 + '\n')

# output sink 1: console
print('\nStarting Console Output...')
query = dashboard_stream.writeStream \
    .foreachBatch(print_dashboard) \
    .outputMode('append') \
    .trigger(processingTime='5 seconds') \
    .option('checkpointLocation', f'{CHECKPOINT_DIR}/console') \
    .start()
print('Console output started.')


print('Dashboard is live. Waiting for traffic...')
print('='*80 + '\n')

# wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print('\nStopping Spark session...')
    spark.stop()
    print('Spark session stopped.')

# stop all queries
queries = [query]
for query in queries:
    query.stop()
    print(f'Stopped {query.name}')

print('All queries stopped.')
spark.stop()
print('Spark session stopped.')