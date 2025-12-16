
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan
from pyspark.sql.types import DoubleType
from functools import reduce

class NetworkTrafficPreprocessor:
    def __init__(self, feature_cols=None):
        self.feature_cols = feature_cols

    def normalize_columns(self, df: DataFrame) -> DataFrame:
        """
        Normalize column names by removing leading/trailing whitespace. an handle duplicate columns
        """
        # Get current columns
        current_cols = df.columns
        
        # Build select expressions for unique, cleaned columns
        select_exprs = []
        seen_columns = set()
        
        for col_name in current_cols:
            clean_name = col_name.strip()
            if clean_name not in seen_columns:
                select_exprs.append(col(col_name).alias(clean_name))
                seen_columns.add(clean_name)
            else:
                print(f'Column {clean_name} is a duplicate and will be dropped.')

        return df.select(*select_exprs)

    def cast_and_clean_features(self, df: DataFrame, inference=True) -> DataFrame:
        """
        Cast feature columns to DoubleType and handle invalid values
        """

        # determine feature columns
        if self.feature_cols is None:
            self.feature_cols = [c for c in df.columns if c != 'Label']

        # cast feature columns to DoubleType and handle invalid values
        select_exprs = []
        for col_name in self.feature_cols:
            # Handle null, NaN, and convert to DoubleType
            cleaned_col = when(
                col(col_name).cast(DoubleType()).isNull() | 
                isnan(col(col_name).cast(DoubleType())) |
                col(col_name).cast(DoubleType()).isin(float('inf'), float('-inf')),
                0.0
            ).otherwise(col(col_name).cast(DoubleType())).alias(col_name)
            
            select_exprs.append(cleaned_col)

        # Add target column if not inference
        if not inference and "Label" in df.columns:
            select_exprs.append(col('Label'))

        # Apply transformations in single pass
        df_clean = df.select(*select_exprs)

        # Drop rows where Label is null
        # df_clean = df_clean.filter(col('Label').isNotNull())

        return df_clean
    
    def map_attack_labels(self, df: DataFrame):
        """
        Standardize attack labels to canonical categories.
        Only applies if Label column exists.
        """
        if 'Label' not in df.columns:
            return df
        
        return df.withColumn('Label',
            when(col('Label').like('BENIGN%'), 'BENIGN')
            .when(col('Label').like('%DDoS%'), 'DDoS')
            .when(col('Label').like('%PortScan%'), 'PortScan')
            .when(col('Label').like('%Heartbleed%'), 'Heartbleed')
            .when(col('Label').like('%DoS%'), 'DoS')
            .when(col('Label').like('%Web Attack%'), 'WebAttack')
            .when(col('Label').like('%XSS%'), 'WebAttack')
            .when(col('Label').like('%SQL Injection%'), 'WebAttack')
            .when(col('Label').like('%Brute Force%'), 'WebAttack')
            .when(col('Label').like('%FTP-Patator%'), 'BruteForce')
            .when(col('Label').like('%SSH-Patator%'), 'BruteForce')
            .when(col('Label').like('%Bot%'), 'BotNet')
            .otherwise('BENIGN')
        )

    def preprocess_for_training(self, df:DataFrame) -> DataFrame:
        """
        Preprocess data for training
        """
        print('Preprocessing data for training...')

        # Normalize column names
        df = self.normalize_columns(df)

        # Cast and clean features
        df = self.cast_and_clean_features(df, inference=False)

        # Map attack labels
        df = self.map_attack_labels(df)

        print(f'Preprocessing complete: {len(df.columns)} columns')

        return df

    def preprocess_for_inference(self, df:DataFrame) -> DataFrame:
        """
        Preprocess data for inference
        """
        print('Preprocessing data for inference...')

        # Normalize column names
        df = self.normalize_columns(df)

        # Cast and clean features
        df = self.cast_and_clean_features(df, inference=True)

        # Map attack labels
        if 'Label' in df.columns:
            df = self.map_attack_labels(df)

        print(f'Preprocessing complete: {len(df.columns)} columns')

        return df

def get_preprocessor(feature_cols=None) -> NetworkTrafficPreprocessor:
    """
    Get preprocessor instance
    """
    return NetworkTrafficPreprocessor(feature_cols)