# common/s3_utils.py

from config.settings import INPUT_PREFIX, BRONZE_PREFIX, SILVER_PREFIX, GOLD_PREFIX

def input_path(filename_pattern: str) -> str:
    """
    Return full input-data path for a filename pattern (e.g., 'patients*.csv').
    """
    return f"{INPUT_PREFIX}/{filename_pattern}"

def bronze_path(dataset_name: str) -> str:
    """
    Return bronze path for a dataset (e.g., 'patients').
    """
    return f"{BRONZE_PREFIX}/{dataset_name}"

def silver_path(dataset_name: str) -> str:
    """
    Return silver path for a dataset (e.g., 'claims').
    """
    return f"{SILVER_PREFIX}/{dataset_name}"

def gold_path(dataset_name: str) -> str:
    return f"{GOLD_PREFIX}/{dataset_name}"
