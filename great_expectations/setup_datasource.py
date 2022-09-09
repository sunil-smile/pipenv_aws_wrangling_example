from typing import List

from ruamel import yaml

import great_expectations as ge
from great_expectations import rule_based_profiler
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.usage_statistics.usage_statistics import UsageStatisticsHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.rule_based_profiler import RuleBasedProfiler, RuleBasedProfilerResult

datacontext_backend_path = "/mnt/c/Users/sunilprasath.elangov/VS_Code_Repo/pipenv_aws_wrangling_example/dq_check_python/backend"

## Setting up datacontext config ,something similar to spark config
store_backend_defaults = FilesystemStoreBackendDefaults(root_directory=datacontext_backend_path)
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = BaseDataContext(project_config=data_context_config)


##setting datasource for the test dataset
datasource_config = {
    "name": "file_datasource",
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
            "base_directory": "test_dataset",
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
            "base_directory": "test_dataset/",
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
            },
        },
    },
}


context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)


inferred_batch_request = BatchRequest(
    datasource_name="file_datasource",
    data_connector_name="yellowtrip_inferred_data_connector_name",
    data_asset_name="yellow_trip",
    data_connector_query={"batch_filter_parameters": {"year": "2021", "month": "01"}},
)
validator = context.get_validator(batch_request=inferred_batch_request)
# print(validator.active_batch_definition)
print(validator.columns)

configured_batch_request = BatchRequest(
    datasource_name="file_datasource",
    data_connector_name="yellowtrip_configured_data_connector_name",
    data_asset_name="green_trip",
    data_connector_query={"batch_filter_parameters": {"month": "2022-01"}},
)
validator = context.get_validator(batch_request=configured_batch_request)
print(validator.active_batch_definition)


print(validator.head(5))

# runtime_batch_request = RuntimeBatchRequest(
#     datasource_name="taxi_datasource",
#     data_connector_name="default_runtime_data_connector_name",
#     data_asset_name="<YOUR MEANINGFUL NAME>",  # This can be anything that identifies this data_asset for you
#     runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
#     batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER>"},
# )


# context.create_expectation_suite(expectation_suite_name="test_suite", overwrite_existing=True)
# validator = context.get_validator(
#     batch_request=runtime_batch_request, expectation_suite_name="test_suite"
# )
# print(validator.head(30))


# # build data docs
# context.build_data_docs()
