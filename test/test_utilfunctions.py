import numpy as np
import pandas as pd
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.utils.utilfunctions import UtilFunctions

# TODO: able to import src.utils when this file is placed in test folder ,
# when i keep it in test/utils/ folder its not able to import src package

# module level
@pytest.fixture(scope="module")
def util_functions_obj() -> UtilFunctions:
    util_functions = UtilFunctions(func_name="sunil")
    return util_functions


# this spark_session parameter we are getting it from session level method defined in conftest.py file as spark_session()
def test_to_uppercase(spark_session: SparkSession):
    input_schema = StructType(
        [
            StructField("stock", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("buyer", StringType(), True),
        ]
    )
    inp_data = [
        {"stock": "AA", "price": 200, "buyer": "sunil"},
        {"stock": "BB", "price": 600, "buyer": "Vidhya"},
        {"stock": "CC", "price": 750, "buyer": "MaHathi"},
    ]
    exp_output_data = [
        {"stock": "AA", "price": 200, "buyer": "SUNIL"},
        {"stock": "BB", "price": 600, "buyer": "VIDHYA"},
        {"stock": "CC", "price": 750, "buyer": "MAHATHI"},
    ]
    inp_df = spark_session.createDataFrame(data=inp_data, schema=input_schema)
    exp_out_df = spark_session.createDataFrame(data=exp_output_data, schema=input_schema)

    act_out_df = UtilFunctions._to_uppercase(df=inp_df, columns_to_transform=["stock", "buyer"])
    # chispa function which helps for comparing the dataframes
    assert_df_equality(exp_out_df, act_out_df, ignore_row_order=True)
    # this is to compare value of the spark dataframe
    assert sorted(exp_out_df.collect()) == sorted(act_out_df.collect())


def test__pd_to_uppercase(util_functions_obj: UtilFunctions):
    input_df = pd.DataFrame(
        {
            "stock": pd.Series(data=["AA", "CC"], index=[0, 2], dtype="str"),
            "price": pd.Series(data=[200, 600, 750], dtype="int"),
            "buyer": pd.Series(data=["sunil", "VidhyA", "MaHathi"], dtype="str"),
        }
    )

    exp_out_df = pd.DataFrame(
        {
            "stock": pd.Series(data=["AA", np.NAN, "CC"], dtype="str"),
            "price": pd.Series(data=[200, 600, 750], dtype="int"),
            "buyer": pd.Series(data=["SUNIL", "VIDHYA", "MAHATHI"], dtype="str"),
        }
    )
    exp_out_df.sort_index(inplace=True, ascending=True)

    act_out_df = util_functions_obj._pd_to_uppercase(
        df=input_df, columns_to_transform=["stock", "buyer"]
    )
    act_out_df.sort_index(inplace=True, ascending=True)

    # this is to compare value of the spark dataframe
    pd.testing.assert_frame_equal(act_out_df, exp_out_df)
