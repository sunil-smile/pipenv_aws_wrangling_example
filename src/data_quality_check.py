import pandas as pd
import pyspark.sql.functions as spark_func
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.utils import AnalysisException

from src.utils.sparkutils import SparkUtilFunctions

spark_session = SparkUtilFunctions._start_spark_session(app_name="dq_check_app")

raw_df = spark_session.read.parquet(
    "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/test_dataset/yellow_trip/"
)
print(raw_df.columns)
# raw_df.show(10)
