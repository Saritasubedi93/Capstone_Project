# common/spark_utils.py

from pyspark.sql import SparkSession

def create_spark(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession with S3A configured.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .getOrCreate()
    )
    return spark
