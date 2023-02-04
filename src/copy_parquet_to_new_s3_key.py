import step_utils
import pyspark.sql.functions as F

def copy_parquet_to_new_s3_key(bucket, original_key, new_key):
    
    spark = step_utils.get_spark_session()

    df = spark.read.parquet(f"s3://{bucket}/{original_key}")
    
    df.withColumn('move_ts', F.current_timestamp())

    df.write.parquet(f"s3://{bucket}/{new_key}", mode="overwrite", compression="snappy")

if __name__ == "__main__":
    copy_parquet_to_new_s3_key('production_bucket', 'original_key/file.parquet', 'new_key/new_filename.parquet')