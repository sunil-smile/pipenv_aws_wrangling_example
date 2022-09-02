import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as F

from src.utils.sparkutils import SparkUtilFunctions

spark_session = SparkUtilFunctions._start_spark_session(app_name="test_app")

df = pd.DataFrame(
    np.random.randint(0, 100, size=(100000, 8)),
    columns=["col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"],
)

mem = df.memory_usage().sum() / 1024**2
print("Memory usage of dataframe is {:.2f} MB".format(mem))

df.loc[df["col_1"] > 10, "col_1"] = 99

spark_session = SparkUtilFunctions._start_spark_session(app_name="test_app")
sparkDF = spark_session.createDataFrame(df)
sparkDF.groupBy(F.spark_partition_id()).count().show()
print(sparkDF.rdd.getNumPartitions())
sparkDF.repartition(67, "col_1")
print(sparkDF.rdd.getNumPartitions())
time.sleep(80)
