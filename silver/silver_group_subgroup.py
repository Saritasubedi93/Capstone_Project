# silver/silver_group_subgroup.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_group_subgroup_silver(spark: SparkSession) -> None:
    # Read Bronze grpsubgrp parquet
    df = spark.read.parquet(bronze_path("group_subgroup"))
    show_nulls(df, "group_subgroup bronze")

    # Clean & standardize
    df = (
        df.na.fill("NA", ["SubGrp_ID", "Grp_Id"])
          .dropDuplicates(["SubGrp_ID", "Grp_Id"])
          .withColumn("SubGrp_ID", upper(col("SubGrp_ID")))
          .withColumn("Grp_Id", upper(col("Grp_Id")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    # Write Silver
    df.write.mode("overwrite").parquet(silver_path("group_subgroup"))
    print("Silver group_subgroup written to", silver_path("group_subgroup"))
