"""This module all the utility functions for the usecase is defined"""
from datetime import datetime
import io
import boto3
import pandas as pd

import awswrangler as wr


class UtilFunctions:
    """
    Utility Class function
    """

    def __init__(self) -> None:
        pass

    def read_s3_file(self, bucketname: str, objectname: str) -> io.BytesIO:
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

    def write_s3_parq_file(
        self, my_session: boto3.Session, bucketname: str, objectname: str, dataframe: pd.DataFrame
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

    def create_athena_database_if_not_exists(
        self, my_session: boto3.Session, database_name: str, dummy: str = "dummy"
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
