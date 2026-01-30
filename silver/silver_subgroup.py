# silver/silver_subgroup.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_subgroup_silver(spark: SparkSession) -> None:
    # Read Bronze subgroup parquet
    df = spark.read.parquet(bronze_path("subgroup"))
    show_nulls(df, "subgroup bronze")

    # Clean & standardize
    df = (
        df.na.fill("NA", ["SubGrp_id", "SubGrp_Name"])
          .na.fill(0, ["Monthly_premium"])
          .dropDuplicates(["SubGrp_id"])
          .withColumn("SubGrp_id", upper(col("SubGrp_id")))
          .withColumn("SubGrp_Name", upper(col("SubGrp_Name")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    # Write Silver
    df.write.mode("overwrite").parquet(silver_path("subgroup"))
    print("Silver subgroup written to", silver_path("subgroup"))
