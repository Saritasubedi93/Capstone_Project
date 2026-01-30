# silver/silver_suscribers.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    concat_ws,
    current_timestamp
)

from common.s3_utils import bronze_path, silver_path
from common.profiling_utils import show_nulls


def run_subscribers_silver(spark: SparkSession) -> None:
    df = spark.read.parquet(bronze_path("subscriber"))
    show_nulls(df, "subscriber bronze")

    # Rename "sub _id" -> "sub_id" once
    df = df.withColumnRenamed("sub _id", "sub_id")

    # Optional full name
    df = df.withColumn(
        "subscriber_name",
        concat_ws(" ", col("first_name"), col("last_name"))
    )

    df = (
        df.na.fill("NA", [
                "first_name",
                "last_name",
                "Street",
                "Birth_date",
                "Gender",
                "Phone",
                "Country",
                "City",
                "Zip Code",
                "Subgrp_id",
                "Elig_ind",
                "eff_date",
                "term_date"
            ])
          .dropDuplicates(["sub_id"])             # now matches renamed column
          .withColumn("Gender", upper(col("Gender")))
          .withColumn("Country", upper(col("Country")))
          .withColumn("City", upper(col("City")))
          .withColumn("Elig_ind", upper(col("Elig_ind")))
          .withColumn("updated_timestamp", current_timestamp())
    )

    df.write.mode("overwrite").parquet(silver_path("subscriber"))
    print("Silver subscriber written to", silver_path("subscriber"))
