# gold/gold_dim_subgroup.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from common.s3_utils import silver_path, gold_path


def build_dim_subgroup(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("subgroup"))

    dim = (
        df.select(
            col("SubGrp_id").alias("subgrp_id"),
            col("SubGrp_Name").alias("subgrp_name"),
            col("Monthly_Premium").alias("monthly_premium"),
        )
        .withColumn("updated_timestamp", current_timestamp())
        .dropDuplicates(["subgrp_id"])
    )

    dim.write.mode("overwrite").parquet(gold_path("dim_subgroup"))
    print("Gold dim_subgroup written to", gold_path("dim_subgroup"))
