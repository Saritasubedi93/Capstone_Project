# silver/silver_group.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_group_silver(spark: SparkSession) -> None:
    # Read Bronze group parquet
    df = spark.read.parquet(bronze_path("group"))
    show_nulls(df, "group bronze")

    # Clean & standardize
    df = (
        df.na.fill("NA", ["country", "zipcode", "Grp_Id", "Grp_Name", "Grp_Type", "city", "year"])
          .dropDuplicates(["Grp_Id"])
          .withColumn("country", upper(col("country")))
          .withColumn("city", upper(col("city")))
          .withColumn("Grp_Name", upper(col("Grp_Name")))
          .withColumn("Grp_Type", upper(col("Grp_Type")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    # Write Silver
    df.write.mode("overwrite").parquet(silver_path("group"))
    print("Silver group written to", silver_path("group"))
