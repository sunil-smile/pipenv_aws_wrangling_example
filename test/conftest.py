import pytest
from pyspark.sql import SparkSession
from src.utils.utilfunctions import UtilFunctions


# Setting session level values
# this should be with file name as 'conftest.py'
# The fixtures that you will define will be shared among all tests in your test suite
@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    return spark
