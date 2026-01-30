# gold/gold_bridge_group_subgroup.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from common.s3_utils import silver_path, gold_path


def build_bridge_group_subgroup(spark: SparkSession) -> None:
    df = spark.read.parquet(silver_path("group_subgroup"))

    bridge = (
        df.select(
            col("Grp_Id").alias("grp_id"),
            col("SubGrp_ID").alias("subgrp_id"),
        )
        .dropDuplicates(["grp_id", "subgrp_id"])
    )

    bridge.write.mode("overwrite").parquet(gold_path("bridge_group_subgroup"))
    print("Gold bridge_group_subgroup written to", gold_path("bridge_group_subgroup"))
