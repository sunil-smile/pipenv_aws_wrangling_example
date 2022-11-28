from multiprocessing import context
from typing import List
from urllib import request

import pandas as pd
import yaml

import great_expectations as ge
from great_expectations import rule_based_profiler
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.core.usage_statistics.usage_statistics import UsageStatisticsHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    FilesystemStoreBackendDefaults,
)
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.rule_based_profiler import RuleBasedProfiler, RuleBasedProfilerResult

# after executing the 'great-expectations init' it creates all the required files
context = ge.get_context()
print(context.get_config())

# datacontext_backend_path = "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/dq_great_expectations"
# store_backend_defaults = FilesystemStoreBackendDefaults(root_directory=datacontext_backend_path)

# data_context_config = DataContextConfig(store_backend_defaults=store_backend_defaults)
# context_new = BaseDataContext(project_config=data_context_config)

# In filesystems, an InferredAssetDataConnector infers the data_asset_name by using a regex that takes advantage of patterns that exist in the filename or folder structure. If your source data system is designed so that it can easily be parsed by regex, this will allow new data to be included by the Datasource automatically. The InferredAssetSqlDataConnector provides similar functionality for SQL based source data systems.
# A ConfiguredAssetDataConnector, which allows you to have the most fine-tuning by requiring an explicit listing of each Data Asset you want to connect to.
# A RuntimeDataConnector which enables you to use a RuntimeBatchRequest to wrap either an in-memory dataframe, filepath, or SQL query.
##setting datasource for the test dataset
trip_datasource_name = "trip_datasource"
trip_datasource_config = {
    "name": trip_datasource_name,
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "SparkDFExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "yellowtrip_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/test_dataset",
            "glob_directive": "*/*.parquet",
            "default_regex": {
                "group_names": [
                    "data_asset_name",
                    "year",
                    "month",
                ],
                "pattern": r"(.*)/yellow_tripdata_(\d{4})-(\d{2})\.parquet",
            },
        },
        "yellowtrip_configured_data_connector_name": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/test_dataset/",
            "glob_directive": "*",
            "assets": {
                "yellow_trip": {
                    "base_directory": "yellow_trip/",
                    "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.parquet",
                    "group_names": ["year", "month"],
                },
                "green_trip": {
                    "base_directory": "green_trip/",
                    "pattern": r"green_tripdata_(.*)\.parquet",
                    "group_names": ["month"],
                },
                # "batch_spec_passthrough": {
                #     "reader_method": "csv",
                #     "reader_options": {
                #         "header": True,
                #         "inferSchema": True,
                #     },
                # },
            },
        },
    },
}

# test the datasource config
context.test_yaml_config(yaml.dump(trip_datasource_config))
# add source to the context
# add_datasource only if it doesn't already exist in your Data Context
try:
    context.get_datasource(trip_datasource_config["name"])
except ValueError:
    context.add_datasource(**trip_datasource_config)
else:
    datasource_name = trip_datasource_config["name"]
    print(f"The datasource {datasource_name} already exists in your Data Context!")

ds = context.get_datasource(trip_datasource_name)
print(ds.config)

# Single vs Multibatches
# single Batch of data
# --------------------
# Validating data, you will need to specify a single Batch of data . If we pass multiple batches it takes the last batch for validataion
# Runtime Data Connector does not support handling multiple Batches of data
# to quickly create some Expectations as part of an early Expectation Suite
data_connector_query_2021 = {"batch_filter_parameters": {"year": "2021", "month": "01"}}
inferred_batch_request = BatchRequest(
    datasource_name=trip_datasource_name,
    data_connector_name="yellowtrip_inferred_data_connector_name",
    data_asset_name="yellow_trip",
    data_connector_query=data_connector_query_2021,
)
inferred_batch_validator = context.get_validator(batch_request=inferred_batch_request)
print(inferred_batch_validator.active_batch_definition.get("data_asset_name"))
print(inferred_batch_validator.head(5))

data_connector_query_2022_01 = {"batch_filter_parameters": {"month": "2022-01"}}
configured_batch_request = BatchRequest(
    datasource_name=trip_datasource_name,
    data_connector_name="yellowtrip_configured_data_connector_name",
    data_asset_name="green_trip",
    data_connector_query=data_connector_query_2022_01,
    limit=2,
)
configured_batch_validator = context.get_validator(batch_request=configured_batch_request)
print(configured_batch_validator.active_batch_definition)
print(configured_batch_validator.head(5))

# Multiple Batch of data
# ----------------------
# Data Assistant to generate an Expectation Suite that is populated with Expectations for you
# batch request with\without any conditions , allows to create multi batch.
# add conditions to return multi batches , for instance filter on year which returns 12 files
multiple_batch_request = BatchRequest(
    datasource_name=trip_datasource_name,
    data_connector_name="yellowtrip_inferred_data_connector_name",
    data_asset_name="yellow_trip",
)

multiple_batch_validator = context.get_validator(batch_request=multiple_batch_request)
print(multiple_batch_validator.head(20))

# Getting the data
# inferred_batch_request.batch_spec_passthrough["splitter_method"] = "split_on_column_value"
# inferred_batch_request.batch_spec_passthrough["splitter_kwargs"] = {
#     "column_name": "passenger_count",
#     "batch_identifiers": {"passenger_count": 2},
# }
# inferred_batch_request.batch_spec_passthrough["sampling_method"] = "_sample_using_random"
# inferred_batch_request.batch_spec_passthrough["sampling_kwargs"] = {"p": 1.0e-1}


# Create Expectations
# interactive workflow

# manually define your Expectations , based on the knowledge about the dataset
manual_expectation_suite = context.create_expectation_suite(
    expectation_suite_name="manual_expectation", overwrite_existing=True
)
expectation_configuration = ExpectationConfiguration(
    # Name of expectation type being added
    # https://greatexpectations.io/expectations
    expectation_type="expect_column_to_exist",
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and
    # Keyword Arguments of this Expectation Type
    kwargs={
        "column_list": [
            "VendorID",
            "RatecodeID",
            "tip_amount:",
            "tolls_amount",
            "total_amount",
        ]
    },
    # This is how you can optionally add a comment about this expectation.
    # It will be rendered in Data Docs.
    # See this guide for details:
    # `How to add comments to Expectations and display them in Data Docs`.
    meta={
        "notes": {
            "format": "markdown",
            "content": "Expected columns are present in the dataset",
        }
    },
)
manual_expectation_suite.add_expectation(expectation_configuration=expectation_configuration)
context.save_expectation_suite(expectation_suite=manual_expectation_suite)
print(manual_expectation_suite.expectations)

# Profiler workflow
# create a new Expectation Suite by profiling your data with the User Configurable Profiler
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler

PROFILER_EXPECTATION = "profile_expectation"
profiler_expectation_suite = context.create_expectation_suite(
    expectation_suite_name=PROFILER_EXPECTATION, overwrite_existing=True
)

# Passing multi batch for creating profile
# profiler = UserConfigurableProfiler(profile_dataset=multiple_batch_request)
# customizing the profiler
excluded_expectations = ["expect_column_quantile_values_to_be_between"]
ignored_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "trip_distance",
]
not_null_only = True
table_expectations_only = False
value_set_threshold = "unique"
profiler = UserConfigurableProfiler(
    profile_dataset=multiple_batch_validator,
    excluded_expectations=excluded_expectations,
    ignored_columns=ignored_columns,
    not_null_only=not_null_only,
    table_expectations_only=table_expectations_only,
    value_set_threshold=value_set_threshold,
)
profiler_expectation_suite = profiler.build_suite()
# # written custom methods
# print(inferred_batch_validator.get_expectation_suite(discard_failed_expectations=False))
# print(inferred_batch_validator.get_expectation_suite())

multiple_batch_validator.expectation_suite_name = PROFILER_EXPECTATION
multiple_batch_validator.save_expectation_suite(discard_failed_expectations=False)
