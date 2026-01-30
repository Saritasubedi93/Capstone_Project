# gold/gold_dim_group.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import silver_path, gold_path


def build_dim_group(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("group"))

    dim = (
        df.select(
            col("Grp_Id").alias("grp_id"),
            col("Grp_Name").alias("grp_name"),
            col("Grp_Type").alias("grp_type"),
            col("Country").alias("country"),
            col("city"),
            col("zipcode"),
            col("year"),
            col("premium_written"),
        )
        .withColumn("country", upper(col("country")))
        .withColumn("city", upper(col("city")))
        .withColumn("grp_type", upper(col("grp_type")))
        .withColumn("updated_timestamp", current_timestamp())
        .dropDuplicates(["grp_id", "year"])
    )

    dim.write.mode("overwrite").parquet(gold_path("dim_group"))
    print("Gold dim_group written to", gold_path("dim_group"))
