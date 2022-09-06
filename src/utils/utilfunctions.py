"""This module all the utility functions for the usecase is defined"""
import io
from datetime import datetime
from typing import List, Optional

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class UtilFunctions:
    """
    Utility Class function
    """

    # class variable
    class_name: str

    def __init__(self, func_name: str) -> None:
        # instance or object variable
        self.func_name = func_name

    # this method is to print the object specific value ,instead of hexadecimal values
    def __repr__(self) -> str:
        return f"the name of the function is {self.func_name}"

    # if we define @classmethod decorator is used to declare a method in the class as a class method that can be called using ClassName. MethodName() .
    # The class method can also be called using an object of the class.
    # UtilFunctions.create_athena_database_if_not_exists
    # since its a class method , we can use cls var
    @classmethod
    def read_s3_file(cls, bucketname: str, objectname: str) -> io.BytesIO:
        """This function to read the files in S3 bucket

        Arguments:
                bucketname :str        -_description_
                objectname :str        -_description_

        Returns:
                io.BytesIO         -_description_
        """
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucketname, Key=objectname)
        return io.BytesIO(response["Body"].read())

    @classmethod
    def write_s3_parq_file(
        cls,
        my_session: boto3.Session,
        bucketname: str,
        objectname: str,
        dataframe: pd.DataFrame,
    ) -> None:
        """The function write the pandas dataframe as parquet file in S3

        Arguments:
                my_session :boto3.Session        -_description_
                bucketname :str        -_description_
                objectname :str        -_description_
                dataframe :pd.DataFrame        -_description_

        Returns:
        """
        dataframe["extracted_timestamp"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        # dataframe.info(verbose=True)
        wr.s3.to_parquet(
            df=dataframe,
            path=f"s3://{bucketname}/{objectname}",
            dataset=True,
            mode="overwrite",
            boto3_session=my_session,
        )

    @classmethod
    def create_athena_database_if_not_exists(
        cls, my_session: boto3.Session, database_name: str, dummy: str = "dummy"
    ) -> None:
        """This function _summary_

        Arguments:
                my_session :boto3.Session
                    description = _description_
                database_name :str
                    description = _description_

        Optional Arguments:
                dummy:str
                    default value="dummy"
                    description = _description_

        Returns:
        """
        print(dummy)
        databases = wr.catalog.databases()
        if database_name not in databases["Database"].tolist():
            wr.catalog.create_database(
                name=database_name, description="Raw zone database", boto3_session=my_session
            )
            print(f"Database {database_name} created")
        else:
            print(f"Database {database_name} already exists")

    @classmethod
    def create_spark_session(cls, app_name: str) -> SparkSession:
        """This function creates the spark session

        Arguments:
                appName :str
                    description = Name of the session
        Returns:
                SparkSession         - returns the spark session
        """
        print("calling function " + app_name)
        spark_session = SparkSession.builder.appName(app_name).getOrCreate()
        print("executing the function")
        return spark_session

    @classmethod
    def create_df_from_file(
        cls, spark: SparkSession, path: str, file_type: str
    ) -> Optional[DataFrame]:
        """This function creates the spark dataframe based on the input file

        Arguments:
                spark :SparkSession
                    description = spark session
                path :str
                    description = path to the input file
                file_type :str
                    description = type of the file

        Returns:
                Optional[DataFrame]         - it returns the spark dataframe if the file type is csv or parquet
        """

        def df_from_parquet(spark: SparkSession, path: str) -> DataFrame:
            if isinstance(spark, SparkSession) and isinstance(path, str):
                df = spark.read.format("parquet").option("header", "true").load(path)
                return df

        def df_from_csv(spark: SparkSession, path: str) -> DataFrame:
            if isinstance(spark, SparkSession) and isinstance(path, str):
                df = spark.read.format("csv").option("header", "true").load(path)
                return df

        return (
            df_from_csv(spark, path)
            if file_type == "csv"
            else df_from_parquet(spark, path)
            if file_type == "parquet"
            else None
        )

    @classmethod
    def _to_uppercase(cls, df: DataFrame, columns_to_transform: List[str]) -> DataFrame:
        """This function _summary_

        Arguments:
                df : DataFrame
                    description =  spark dataframe
                columns_to_transform : list
                    description = List of column names for which the values has to be transformed to uppercase

        Optional Arguments:

        Returns:
                DataFrame         = Returns the converted spark dataframe
        """

        # Loop through columns to transform and convert to uppercase
        for column in columns_to_transform:
            if column in df.columns:
                df = df.withColumn(column, F.upper(F.col(column)))

        return df

    # private method example
    def __test_private(self) -> None:
        print("test private method")

    # function encapsulation
    # without _ in front => public (access from anywhere)
    # one _ in front =>  protected (accessed from the package)
    # two __ in front => private (access only from that class and inherited class)
    # this same applies to variables as well
    def _pd_to_uppercase(self, df: pd.DataFrame, columns_to_transform: List[str]) -> pd.DataFrame:
        """Uppercase the columns provided in the dataframe
        Args:
            df (DataFrame): Input Dataframe
            columns_to_transform (List): List of columns to uppercase
        Returns:
            DataFrame: The transformed DataFrame
        """
        self.__test_private()
        # Loop through columns to transform and convert to uppercase
        for column in columns_to_transform:
            if column in df.columns:
                df[column] = df[column].str.upper()
        return df

    @classmethod
    def __convert_categorical(cls, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """This function converts the Python dataframe object datatype to categorical if the average of unique values is less than 0.33

        Arguments:
                df : pd.DataFrame
                    description = Input pandas dataframe
                col : str
                    description = Object data type column name

        Returns:
                pd.DataFrame         =  returns the dataframe which changes object datatype to categorical if the conditions are met
        """
        assert isinstance(df, pd.DataFrame), "{df} is not a Pandas DataFrame"
        num_unique_values = df[col].nunique()
        num_total_values = len(df[col])
        if (num_total_values != 0) and (num_unique_values / num_total_values < 0.33):
            df.loc[:, col] = df[col].astype("category")
        return df

    # https://www.mikulskibartosz.name/how-to-reduce-memory-usage-in-pandas/
    @classmethod
    def reduce_mem_usage(cls, df: pd.DataFrame) -> pd.DataFrame:
        """This function optimize the pandas dataframe datatypes based on the min and max value range of the column assign the correct
        datatype ,for instance downcasting int64 to int8 . which reduces the memory usage

        Arguments:
                df : pd.DataFrame
                    description = Input pandas dataframe

        Returns:
                pd.DataFrame         = returns the dataframe with correct memory optimized datatype
        """
        start_mem = df.memory_usage().sum() / 1024**2
        print("Memory usage of dataframe is {:.2f} MB".format(start_mem))
        for col in df.columns:
            col_type = df[col].dtype
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == "int":
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.uint8).min and c_max < np.iinfo(np.uint8).max:
                    df[col] = df[col].astype(np.uint8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.uint16).min and c_max < np.iinfo(np.uint16).max:
                    df[col] = df[col].astype(np.uint16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.uint32).min and c_max < np.iinfo(np.uint32).max:
                    df[col] = df[col].astype(np.uint32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
                elif c_min > np.iinfo(np.uint64).min and c_max < np.iinfo(np.uint64).max:
                    df[col] = df[col].astype(np.uint64)
            elif str(col_type)[:5] == "float":
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df = cls.__convert_categorical(df, col)

        end_mem = df.memory_usage().sum() / 1024**2
        print("Memory usage after optimization is: {:.2f} MB".format(end_mem))
        print("Decreased by {:.1f}%".format(100 * (start_mem - end_mem) / start_mem))

        return df
