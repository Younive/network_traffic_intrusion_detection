from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from preprocessing import get_preprocessor
from config import FILE_PATHS, TRAIN_DATA_PATH

spark = SparkSession.builder \
    .appName("NetworkIntrusionDetector") \
    .config('spark.driver.memory', '4g') \
    .config('spark.sql.csv.parser.columnPruning.enabled', 'false') \
    .config('spark.sql.files.ignoreCorruptFiles', 'false') \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
# Create preprocessor
preprocessor = get_preprocessor()

def process_train_data():
    all_samples = []
    
    for file in FILE_PATHS:
        print(f"\nProcessing {file}...")
        
        # Read raw CSV
        df = spark.read.csv(file, header=True, inferSchema=True)
        
        # Apply preprocessing pipeline
        df_clean = preprocessor.preprocess_for_training(df)
        
        # Filter Heartbleed
        df_clean = df_clean.filter(col('Label') != 'Heartbleed')
        
        # Sampling logic
        rare = df_clean.filter(col('Label').isin(['WebAttack', 'BotNet']))
        common = df_clean.filter(~col('Label').isin(['WebAttack', 'BotNet']))
        common_sample = common.sample(fraction=0.1, seed=42)
        
        sampled = rare.union(common_sample)
        all_samples.append(sampled)
        print(f"  Sampled: {sampled.count()} rows")
    
    # Combine and save
    final_df = all_samples[0]
    for df in all_samples[1:]:
        final_df = final_df.union(df)
    
    print(f"\nTotal: {final_df.count()}")
    
    # Save as Parquet
    final_df.write.mode('overwrite').parquet(TRAIN_DATA_PATH)
    print(f"Saved to {TRAIN_DATA_PATH}")

if __name__ == "__main__":
    process_train_data()
    spark.stop()