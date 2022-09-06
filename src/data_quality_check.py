#%%
import pandas as pd
import pyspark.sql.functions as spark_func
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.utils import AnalysisException

from src.utils.sparkutils import SparkUtilFunctions

spark_session = SparkUtilFunctions._start_spark_session(app_name="dq_check_app")

raw_df = spark_session.read.option("header", True).csv(
    "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/test_dataset/Kickstarter_projects_Feb19.csv"
)
raw_df.createOrReplaceTempView("CAMPAIGNS")
raw_df.groupBy(spark_func.spark_partition_id()).count().show()
print(raw_df.rdd.getNumPartitions())
# Getting the partitions size of the spark
print(spark_session.conf.get("spark.sql.files.maxPartitionBytes"))
# Get the default number of partitions
print(spark_session.sparkContext.defaultParallelism)
raw_df = raw_df.repartition(30)
print(raw_df.rdd.getNumPartitions())
raw_df.groupBy(spark_func.spark_partition_id()).count().show(100, truncate=False)
#%%
raw_test_df = SparkDFDataset(raw_df)

# Raw DF Test 1: Check if mandatory columns exist
MANDATORY_COLUMNS = [
    "id",
    "currency",
    "main_category",
    "launched_at",
    "deadline",
    "country",
    "status",
    "usd_pledged",
]
for column in MANDATORY_COLUMNS:
    try:
        assert raw_test_df.expect_column_to_exist(
            column
        ).success, f"Uh oh! Mandatory column {column} does not exist: FAILED"
        print(f"Column {column} exists : PASSED")
    except AssertionError as e:
        print(e)

# Raw DF Test 2: Check if mandatory columns are not null
for column in MANDATORY_COLUMNS:
    try:
        test_result = raw_test_df.expect_column_values_to_not_be_null(column)
        assert (
            test_result.success
        ), f"Uh oh! {test_result.result['unexpected_count']} of {test_result.result['element_count']} items in column {column} are null: FAILED"
        print(f"All items in column {column} are not null: PASSED")
    except AssertionError as e:
        print(e)
    except AnalysisException as e:
        print(e)

# %%
# Raw DF Test 3: Check if launched_at is a valid datetime format
TIMESTAMP_COLS = ["launched_at", "deadline"]
for column in MANDATORY_COLUMNS:
    try:
        test_result = raw_test_df.expect_column_values_to_not_be_null(column)
        assert (
            test_result.success
        ), f"Uh oh! {test_result.result['unexpected_count']} of {test_result.result['element_count']} items in column {column} are null: FAILED"
        print(f"All items in column {column} are not null: PASSED")
    except AssertionError as e:
        print(e)
    except AnalysisException as e:
        print(e)
test_result = raw_test_df.expect_column_values_to_match_strftime_format(
    "launched_at", "%Y-%m-%d %H:%M:%S"
)
f"""{round(test_result.result['unexpected_percent'], 2)}% is not a valid date time format"""
# %%
raw_test_df_validation = raw_test_df.validate()
print(
    f"""1. Raw dataset validations: {raw_test_df_validation.success}; {raw_test_df_validation.statistics['success_percent']} successful"""
)
