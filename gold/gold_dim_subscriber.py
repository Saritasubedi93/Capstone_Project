# gold/gold_dim_subscriber.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp, to_date

from common.s3_utils import silver_path, gold_path


def build_dim_subscriber(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("subscriber"))

    dim = (
        df.select(
            col("sub_id"),
            col("first_name"),
            col("last_name"),
            col("subscriber_name"),
            col("Gender").alias("gender"),
            col("Birth_date").cast("date").alias("birth_date"),
            col("Phone").alias("phone"),
            col("Country").alias("country"),
            col("City").alias("city"),
            col("Zip Code").cast("int").alias("zip_code"),
            col("Subgrp_id").alias("subgrp_id"),
            col("Elig_ind").alias("elig_ind"),
            col("eff_date"),
            col("term_date"),
        )
        .withColumn("gender", upper(col("gender")))
        .withColumn("country", upper(col("country")))
        .withColumn("city", upper(col("city")))
        .withColumn("elig_ind", upper(col("elig_ind")))
        .withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd"))
        .withColumn("eff_date", to_date(col("eff_date"), "yyyy-MM-dd"))
        .withColumn("term_date", to_date(col("term_date"), "yyyy-MM-dd"))
        .withColumn("updated_timestamp", current_timestamp())
        .dropDuplicates(["sub_id"])
    )

    dim.write.mode("overwrite").parquet(gold_path("dim_subscriber"))
    print("Gold dim_subscriber written to", gold_path("dim_subscriber"))
