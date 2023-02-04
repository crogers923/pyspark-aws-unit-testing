import os, signal, subprocess

import unittest
from pyspark.sql import SparkSession
import boto3
import logging


class PySparkTest(unittest.TestCase):

    @classmethod
    def set_class_vars(cls):
        cls.MOCK_PORT = "5000"
        cls.S3_MOCK_ENDPOINT = f"http://127.0.0.1:{cls.MOCK_PORT}"
        hadoop_aws_jar = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
        aws_java_sdk_jar = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.399/aws-java-sdk-bundle-1.12.399.jar"
        cls.spark = SparkSession.build.master('local[2]').appName('local-test-pyspark')\
            .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}")\
                .getOrCreate()
        cls.TEST_BUCKET = "test_bucket"

    @classmethod 
    def aws_creds(cls):
        os.environ['AWS_ACCESS_KEY_ID'] = 'mocked'
        os.environ['AWS_SECRETE_ACCESS_KEY'] = 'mocked'
        os.environ['AWS_SECURITY_TOKEN'] = 'mocked'
        os.environ['AWS_SESSION_TOKEN'] = 'mocked'
        os.environ['AWS_DEFAULT_REGION'] = 'mocked'

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def build_moto_server_and_aws_mocks(cls):
        cls.process = subprocess.Popen(
            f"moto_server -p{cls.MOCK_PORT}", stdout=subprocess.PIPE,
            shell=True, preexec_fn=os.setsid
        )

    @classmethod
    def create_test_bucket(cls):
        cls.s3 = boto3.resource("s3", endpoint_url = cls.S3_MOCK_ENDPOINT)
        cls.s3.create_bucket(Bucket=cls.TEST_BUCKET)

    @classmethod
    def create_testing_pyspark_session(cls):
        hadoop_conf = cls.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", "mock")
        hadoop_conf.set("fs.s3a.secret.key", "mock")
        hadoop_conf.set("fs.s3.access.key", "mock")
        hadoop_conf.set("fs.s3.secret.key", "mock")
        hadoop_conf.set("fs.s3a.endpoint", cls.S3_MOCK_ENDPOINT)
        hadoop_conf.set("fs.s3.endpoint", cls.S3_MOCK_ENDPOINT)

    @classmethod
    def setUpClass(cls):
        cls.set_class_vars()
        cls.aws_creds()
        cls.suppress_py4j_logging()
        cls.build_moto_server_and_aws_mocks()
        cls.create_test_bucket()
        cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        os.killpg(os.getpgid(cls.process.pid), signal.SIGTERM)

if __name__ == "__main__":
    try:
        unittest.main()
    except:
        PySparkTest.tearDownClass()