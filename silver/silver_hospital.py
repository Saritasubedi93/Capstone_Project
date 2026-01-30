from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_hospital_silver(spark: SparkSession) -> None:
    # Read Bronze hospital parquet
    df = spark.read.parquet(bronze_path("hospital"))
    show_nulls(df, "hospital bronze")

    # Clean and standardize
    df = (
        df.na.fill("NA", ["Hospital_name", "city", "state", "country"])
          .dropDuplicates(["hospital_id"])
          .withColumn("Hospital_name", upper(col("Hospital_name")))
          .withColumn("city", upper(col("city")))
          .withColumn("state", upper(col("state")))
          .withColumn("country", upper(col("country")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    # Write Silver
    df.write.mode("overwrite").parquet(silver_path("hospital"))
    print("Silver hospital written to", silver_path("hospital"))
