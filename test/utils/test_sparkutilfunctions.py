import numpy as np
import pandas as pd
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StringType
from src.utils.sparkutils import SparkUtilFunctions


def test__start_spark_session(spark_session: SparkSession):
    session = SparkUtilFunctions._start_spark_session(app_name="test_app")
    assert type(session) == type(spark_session)


def test__get_corresponding_datatype(spark_session: SparkSession):
    inp_dict = {"col_1": "integer", "col_2": "string", "col_3": "float"}
    exp_dict_output = {"col_1": IntegerType(), "col_2": StringType(), "col_3": FloatType()}
    act_dict_output = SparkUtilFunctions._get_corresponding_datatype(inp_dict)
    assert exp_dict_output == act_dict_output


def test__get_corresponding_datatype_exception(spark_session: SparkSession):
    inp_dict = {"col_1": "integer", "col_2": "string", "col_3": "foat"}
    with pytest.raises(ValueError) as e:
        SparkUtilFunctions._get_corresponding_datatype(inp_dict)
    assert str(e.value) == "Invalid or Unsupported data type"
