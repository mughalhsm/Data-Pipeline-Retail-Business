import boto3 
from botocore.exceptions import ClientError, ParamValidationError
from moto import mock_logs
import pytest
from src.alter_errors import put_metric_filter_func
from unittest.mock import patch
import os

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def logs_client(aws_credentials):
    with mock_logs():
        yield boto3.client("logs", region_name='us-east-1')


def test_that_creation_of_metric_filter_is_success(logs_client):
    # Call with complete command
    put_metric_response = put_metric_filter_func(
            logGroupName ="/aws/lambda/Ingestion_Function5",
            filterName ="errors-filter",
            filterPattern ="ERROR",
            metricTransformations =[
                {
                    "metricValue": "1",
                    "metricNamespace": "Testing_for_errors_Lambda_With_Metric_Filter",
                    "metricName": "errors-metric",
                    "defaultValue": 0
                }
            ]
        )
    print(put_metric_response)
    assert put_metric_response["ResponseMetadata"]["HTTPStatusCode"] == 200




def test_exit_when_incorrect_log_group(logs_client):
    '''quits when called with incorrect log group, cant find resource'''
    with pytest.raises(SystemExit) as see:
           put_metric_filter_func(logGroupName ="/non/existent", filterName ='test', filterPattern ='test', metricTransformations=[
                {
                    "metricValue": "1",
                    "metricNamespace": "Testing_for_errors_Lambda_With_Metric_Filter",
                    "metricName": "errors-metric",
                    "defaultValue": 0
                }
            ]) ## can pass this defined name in script and it will fail
    assert see.type == SystemExit
    assert see.value.code == 'ResourceNotFoundException'



def test_to_fail_when_incomplete_fields_entered(logs_client):
    ''' Call the put_metric_filter command with missing fields '''
    with pytest.raises(TypeError) as te:
        put_metric_response = put_metric_filter_func(
        logGroupName="/aws/lambda/Ingestion_Function5",
        metricTransformations=[
            {
                "metricValue": "1",
                "metricNamespace": "Testing_for_errors_Lambda_With_Metric_Filter",
                "metricName": "errors-metric",
                "defaultValue": 0
            }
        ]
    )
    
    assert te.type == TypeError
    
