from tests.unit.PySparkTest import PySparkTest
from mock import patch
from datetime import datetime

import src.copy_parquet_to_new_s3_key as copy_parquet_to_new_s3_key

class TestCopyParquetToNewS3Key(PySparkTest):\

    @patch('src.step_utils.get_spark_session')
    def test_copy_parquet_to_new_s3_key(self, spark_session_func):
        spark_session_func.return_value = self.spark
        

        values = [(1, "MadeUp Human", "2022-12-31"), (2, "AnotherOne MadeUp", "2023-01-01")]
        columns = ("key", "name", "registration_date")

        df = self.spark.createDataFrame(values, columns)

        df.write.parquet(f"s3://{self.TEST_BUCKET}/original_key/test_file.parquet")
        copy_parquet_to_new_s3_key(self.TEST_BUCKET, 'original_key/test_file.parquet', 'new_key/test_file_new_name.parquet')

        output_df = self.spark.read.parquet(f"s3://{self.TEST_BUCKET}/new_key/test_file_new_name.parquet")
        self.assertTrue(not output_df.rdd.isEmpty())
        self.assertTrue(df.subtract(output_df).rdd.isEmpty())
