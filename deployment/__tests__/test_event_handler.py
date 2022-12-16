from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.create_buckets import Create_resources
from deployment.src.assign_iam import Assign_iam
from deployment.src.event_handler import Create_events
from moto import mock_s3, mock_iam, mock_lambda, mock_logs, mock_events
import boto3
import pytest
from unittest.mock import patch
from botocore.exceptions import ClientError
import os

@patch('deployment.src.event_handler.os')
@mock_events
def test_event_handler_creates_connection_on_instance_creation(os):
    """Mocking environment variables to mimic github secrets and mocking boto3, test that s3 exists"""
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "temp",
        'AWS_SECRET_KEY': "tempSecret"}}
    event = Create_events()
    assert event.events != None

@mock_events
def test_create_schedule_event_creates_enabled_event_of_passed_values():    
    event = Create_events()
    result = event.create_schedule_event("test-schedule",1)
    assert 'RuleArn' in result
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200


@mock_logs
@mock_events
@mock_lambda
@mock_iam
@mock_s3
def test_assign_event_taget_gives_passed_lambda_scheduler_rule():
    create = Create_resources()
    create.create_s3_bucket("code-bucket")
    create.create_s3_bucket("ingest-bucket")
    create.upload_lambda_function_code(code_bucket="code-bucket",folder_path="deployment/__tests__/test_data/lambda2",lambda_name="customLambda")
    permit = Assign_iam()
    permit.create_lambda_role(role_name="test-role")
    permit.attach_execution_role(role_name='test-role')
    permit.create_cloudwatch_logging_policy(lambda_name='customLambda')
    permit.attach_custom_policy(role_name='test-role',policy='cloudwatch-policy-customLambda')
    deploy = Deploy_lambdas()
    role_arn = permit.role_arns['test-role']
    deploy.create_lambda(lambda_name="customLambda",code_bucket="code-bucket",role_arn=role_arn,zip_file="customLambda.zip")
    event = Create_events()
    event.create_schedule_event(schedule_name="test-schedule",minute_count=1)
    lambda_arn = deploy.lambda_arns['customLambda']
    result = event.assign_event_target(schedule_name="test-schedule",target_arn=lambda_arn)
    assert result['FailedEntryCount'] == 0
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

