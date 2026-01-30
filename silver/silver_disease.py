# silver/silver_disease.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_disease_silver(spark: SparkSession) -> None:
    df = spark.read.parquet(bronze_path("disease"))
    show_nulls(df, "disease bronze")

    # Fix column name with leading space
    df = df.withColumnRenamed(" Disease_ID", "Disease_ID")

    df = (
        df.na.fill("NA", ["SubGrpID", "Disease_ID", "Disease_name"])
          .dropDuplicates(["Disease_ID"])
          .withColumn("SubGrpID", upper(col("SubGrpID")))
          .withColumn("Disease_name", upper(col("Disease_name")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    df.write.mode("overwrite").parquet(silver_path("disease"))
    print("Silver disease written to", silver_path("disease"))
