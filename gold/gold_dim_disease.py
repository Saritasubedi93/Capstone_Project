# gold/gold_dim_disease.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import silver_path, gold_path


def build_dim_disease(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("disease"))

    dim = (
        df.select(
            col("Disease_ID").alias("disease_id"),
            col("Disease_name").alias("disease_name"),
            col("SubGrpID").alias("subgrp_id"),
        )
        .withColumn("disease_name", upper(col("disease_name")))
        .withColumn("updated_timestamp", current_timestamp())
        .dropDuplicates(["disease_id"])
    )

    dim.write.mode("overwrite").parquet(gold_path("dim_disease"))
    print("Gold dim_disease written to", gold_path("dim_disease"))
