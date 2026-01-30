# common/profiling_utils.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

def profile_nulls(df: DataFrame) -> DataFrame:
    """
    Return a one-row DataFrame with null counts per column.
    """
    exprs = [count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]
    return df.agg(*exprs)

def show_nulls(df: DataFrame, label: str = "") -> None:
    """
    Print null counts in a readable way.
    """
    print(f"Null profile: {label}")
    profile_nulls(df).show(truncate=False)
