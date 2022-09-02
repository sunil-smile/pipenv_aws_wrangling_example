"""This module all the utility functions for the usecase is defined"""

import random
import string
from typing import Any
from unittest import case

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType


class SparkUtilFunctions:
    """
    Utility Class for Spark applications
    """

    @classmethod
    def _start_spark_session(cls, app_name: str) -> SparkSession:
        spark = (
            SparkSession.builder.master("local[*]")
            .config("spark.port.maxRetries", "1000")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.ui.port", "4050")
            .config("spark.app.name", app_name)
            .getOrCreate()
        )
        return spark

    @classmethod
    def __get_data_type(cls, dtype: str) -> DataType:
        dtype = dtype.upper()
        if dtype == "INTEGER":
            return IntegerType()
        elif dtype == "STRING":
            return StringType()
        elif dtype == "BOOLEAN":
            return BooleanType()
        elif dtype == "DOUBLE":
            return DoubleType()
        elif dtype == "FLOAT":
            return FloatType()
        else:
            raise ValueError("Invalid or Unsupported data type")

    @classmethod
    def _get_corresponding_datatype(cls, req_type: dict) -> dict:
        for col in req_type:
            req_type[col] = cls.__get_data_type(req_type[col])
        return req_type

    @classmethod
    def _random_value(cls, dtype: DataType) -> Any:
        if dtype == IntegerType:
            return random.randint(0, 3)
        elif dtype == StringType:
            letters = string.ascii_lowercase
            return "".join(random.choice(letters) for i in range(12))
        elif dtype == BooleanType:
            return bool(random.getrandbits(1))
        elif dtype == DoubleType:
            return round(random.uniform(1.3, 1.7), 6)
        elif dtype == FloatType:
            return round(random.uniform(1.3, 1.7), 6)
