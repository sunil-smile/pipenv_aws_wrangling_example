from multiprocessing import context
from typing import List
from urllib import request

import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.checkpoint.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.usage_statistics.usage_statistics import UsageStatisticsHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.expectations.expectation import ExpectationConfiguration

# after executing the 'great-expectations init' it creates all the required files
context = ge.get_context()

# Setup runtime data connector.
runtime_datasource_name = "runtime_test_datasource"
runtime_datasource_config = {
    "name": runtime_datasource_name,
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "spark_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}
context.test_yaml_config(yaml.dump(runtime_datasource_config))
context.add_datasource(**runtime_datasource_config)

df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "e", "b"], index=None)

RUNTIME_MANUAL_EXCEPTION_NAME = "runtime_manual_expectation"
runtime_example_expectation_configuration = ExpectationConfiguration(
    # Name of expectation type being added
    # https://greatexpectations.io/expectations
    expectation_type="expect_table_columns_to_match_ordered_list",
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and
    # Keyword Arguments of this Expectation Type
    kwargs={"column_list": ["a", "b", "c"]},
    # This is how you can optionally add a comment about this expectation.
    # It will be rendered in Data Docs.
    # See this guide for details:
    # `How to add comments to Expectations and display them in Data Docs`.
    meta={
        "notes": {
            "format": "markdown",
            "content": "Expected columns are present in the order of a,b,c",
        }
    },
)
# Add the Expectation to the suite

runtime_manual_expectation_suite = context.create_expectation_suite(
    expectation_suite_name=RUNTIME_MANUAL_EXCEPTION_NAME, overwrite_existing=True
)
runtime_manual_expectation_suite.add_expectation(
    expectation_configuration=runtime_example_expectation_configuration
)
context.save_expectation_suite(expectation_suite=runtime_manual_expectation_suite)

# validate your exception suite
runtime_batch_request = RuntimeBatchRequest(
    datasource_name=runtime_datasource_name,
    data_connector_name="spark_runtime_data_connector_name",
    data_asset_name="spark_runtime_asset_name",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df},  # df is your dataframe
    batch_identifiers={"default_identifier_name": "default_identifier_name"},
)

data_assistant_result = context.assistants.onboarding.run(
    batch_request=runtime_batch_request,
)
DATA_ASS_EXPEC = "data_assistant_expectations"
expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name=DATA_ASS_EXPEC
)
context.save_expectation_suite(
    expectation_suite=expectation_suite, discard_failed_expectations=False
)

# Set up and run a Simple Checkpoint for ad hoc validation of our data
checkpoint_config = {
    "name": "my_missing_keys_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": runtime_datasource_name,
                "data_connector_name": "spark_runtime_data_connector_name",
                "data_asset_name": "checkpoint_runtime_asset_name",
            },
            "expectation_suite_name": DATA_ASS_EXPEC,
        }
    ],
}
context.add_checkpoint(**checkpoint_config)

checkpoint_result = context.run_checkpoint(
    checkpoint_name="my_missing_keys_checkpoint",
    batch_request={
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {"default_identifier_name": "dummy"},
    },
)


# build data docs
context.build_data_docs()


# # Get the only validation_result_identifier from our SimpleCheckpoint run, and open Data Docs to that page
# validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)
