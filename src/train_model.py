from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.storagelevel import StorageLevel
from config import TRAIN_DATA_PATH

# create spark session with increased memory
spark = SparkSession.builder \
    .appName('NetworkIntrusionDetector') \
    .master('local[4]') \
    .config('spark.driver.memory', '6g') \
    .config('spark.driver.maxResultSize', '2g') \
    .config('spark.executor.memory', '4g') \
    .config('spark.memory.fraction', '0.8') \
    .config('spark.memory.storageFraction', '0.3') \
    .config('spark.default.parallelism', '8') \
    .config('spark.sql.shuffle.partitions', '8') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .getOrCreate()

spark.sparkContext.setCheckpointDir('./checkpoints')
spark.sparkContext.setLogLevel('WARN')

# load data
print('Loading Data...')
df = spark.read.parquet(TRAIN_DATA_PATH)

# determine feature columns and target column
feature_cols = [c for c in df.columns if c != 'Label']
target_col = 'Label'

print(f'After cleaning: {df.count()}')

# Checkpoint to break lineage
print('Checkpointing cleaned data...')
df = df.checkpoint()

# Split data into train and test
print('\nSplitting data into train/test...')
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

train_df = train_df.repartition(8).persist(StorageLevel.MEMORY_AND_DISK)
train_count = train_df.count()  # Materialize
print(f'Training samples: {train_count}')

# Repartition test data (don't persist yet)
test_df = test_df.repartition(4)
test_count = test_df.count()
print(f'Test samples: {test_count}')

# build ML pipeline
print('\nBuilding ML pipeline...')
indexer = StringIndexer(inputCol=target_col, outputCol='label_indexed')

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features', handleInvalid='skip')

rf = RandomForestClassifier(
    labelCol='label_indexed', 
    featuresCol='features', 
    numTrees=20,
    maxDepth=5,
    seed=42,
    featureSubsetStrategy='sqrt'
)

pipeline = Pipeline(stages=[indexer, assembler, rf])

# persist train_df to break lineage
train_df.persist(StorageLevel.MEMORY_AND_DISK)

# train model
print('\nTraining model...')
model = pipeline.fit(train_df)

# evaluate model on test set
print('\nEvaluating model on test set...')

# sampling test samples
print('Sampling test samples...')
test_sample = test_df.sample(fraction=0.3, seed=42)

# transform test samples
print('Transforming test samples...')
predictions = (model.transform(test_sample).persist(StorageLevel.MEMORY_AND_DISK))
# force materialization
predictions.count()

evaluator = MulticlassClassificationEvaluator(
    labelCol='label_indexed', 
    predictionCol='prediction'
)

# print evaluation results
for metric in ['accuracy', 'weightedRecall', 'weightedPrecision', 'f1']:
    evaluator.setMetricName(metric)
    score = evaluator.evaluate(predictions)
    print(f'{metric.capitalize():20s}: {score:.4f}')

# Show prediction distribution
print('\nPrediction Distribution (sample):')
predictions.groupBy(target_col, 'prediction').count() \
    .orderBy(target_col, 'prediction') \
    .show(20, truncate=False)

# Unpersist predictions
predictions.unpersist()

# save model
print('\nSaving model...')
model.write().overwrite().save('models/network_intrusion_detector_model')

print('\n✅ Model trained and saved successfully!')
spark.stop()