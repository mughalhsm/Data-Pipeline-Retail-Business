from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.create_buckets import Create_resources, zip_directory
from deployment.src.assign_iam import Assign_iam
from moto import mock_s3, mock_iam, mock_lambda, mock_logs
import boto3
import pytest
from unittest.mock import patch
from botocore.exceptions import ClientError
import os
import zipfile


@patch('deployment.src.create_buckets.os')
@mock_s3
def test_create_resource_creates_connection_on_instance_creation(os):
    """Mocking environment variables to mimic github secrets and mocking boto3, test that s3 exists"""
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "temp",
        'AWS_SECRET_KEY': "tempSecret"}}
    creator = Create_resources()
    assert creator.s3 != None


@pytest.mark.skip("Setup causing keyerror to be triggered instead")
@mock_s3
def test_create_resource_states_the_error_when_a_the_correct_github_secrets_have_not_been_made():
    """Testing the response when an access key is missing using mocked os response"""
    creator = Create_resources()
    result = creator.errors[0]
    assert result == "Failed to find keys 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY' on key 'GITHUB_TOKEN'"


@pytest.mark.skip("Setup causing keyerror to be triggered instead")
@patch('deployment.src.create_buckets.boto3')
@patch('deployment.src.create_buckets.os')
def test_create_resource_states_when_a_user_error_such_as_invalid_password_has_been_made(os, boto):
    """Testing the response when an client error is raised on client creation by intercepting boto's client creation"""
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "temp",
        'AWS_SECRET_KEY': "tempSecret"}}

    class patched_boto:
        def client(*, region_name, aws_access_key_id, aws_secret_access_key):
            raise ClientError(error_response={'Error': {
                              'Message': 'Client error content', 'Code': '123'}}, operation_name="op")
    boto.return_value = patched_boto
    creator = Create_resources()
    result = creator.errors[0]
    assert result == "Client Error : Client error content"


@patch('deployment.src.create_buckets.os')
@mock_s3
def test_buckets_are_created_on_call(os):
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "",
        'AWS_SECRET_KEY': ""}}
    creator = Create_resources()
    creator.create_s3_bucket('test-bucket')
    result = creator.s3.list_buckets()
    assert "test-bucket" == result['Buckets'][0]['Name']


@patch('deployment.src.create_buckets.boto3')
@patch('deployment.src.create_buckets.os')
@mock_s3
def test_buckets_not_created_and_client_error_handled_for_invalid_names(os, boto):
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "",
        'AWS_SECRET_KEY': ""}}

    class patched_client:
        def __init__(*, region_name, aws_access_key_id, aws_secret_access_key):
            pass

        def create_bucket(*, Bucket):
            raise ClientError(error_response={'Error': {
                              'Message': f"Client error content for {Bucket}", 'Code': '123'}}, operation_name="op")
    boto.client.return_value = patched_client
    creator = Create_resources()
    creator.create_s3_bucket('test-bucket')
    result = creator.errors
    assert "Client Error : Client error content for test-bucket" in result


def test_directory_zipped_for_lambda_use():
    zip_directory('deployment/__tests__/test_data/lambda1')
    assert os.path.exists("lambda.zip")
    with zipfile.ZipFile("lambda.zip", "r") as archive:
        files = archive.namelist()
    for file in files:
        assert file in ['main.py', 'src/controller.py', 'src/method.py']


def test_directory_zipped_for_lambda_use_is_replaced_upon_new_zip():
    zip_directory('deployment/__tests__/test_data/lambda1')
    assert os.path.exists("lambda.zip")
    with zipfile.ZipFile("lambda.zip", "r") as archive:
        files = archive.namelist()
    for file in files:
        assert file in ['main.py', 'src/controller.py', 'src/method.py']
    zip_directory('deployment/__tests__/test_data/lambda2')
    assert os.path.exists("lambda.zip")
    with zipfile.ZipFile("lambda.zip", "r") as archive:
        files = archive.namelist()
    for file in files:
        assert file not in ['src/controller.py', 'src/method.py']

def test_zip_directory_includes_pandas_zipped_if_dependency_exists():
    zip_directory('deployment/__tests__/test_data/lambda1', pandas_dependency=True)
    assert os.path.exists("lambda.zip")
    with zipfile.ZipFile("lambda.zip", "r") as archive:
        files = archive.namelist()
    for file in files:
        assert file in ['main.py', 'src/controller.py', 'src/method.py'] or file[:7] == 'pandas/'


@mock_s3
def test_upload_lambda_function_code_zips_appropriate_file_and_uploads_to_appropriate_bucket():
    creator = Create_resources()
    creator.create_s3_bucket("code_bucket")
    creator.upload_lambda_function_code(
        "deployment/__tests__/test_data/lambda1", "code_bucket", "lambda1")
    files = creator.s3.list_objects_v2(Bucket="code_bucket")
    assert "lambda1.zip" in [file['Key'] for file in files['Contents']]


def test_upload_lambda_function_states_error_when_no_valid_bucket_exists(capsys):
    creator = Create_resources()
    creator.upload_lambda_function_code(
        "deployment/__tests__/test_data/lambda1", "code_bucket", "lambda1")
    prints, err = capsys.readouterr()
    assert prints == 'Bucket does not exist. Upload of lambda1 to code_bucket failed\n'

@mock_logs
@mock_lambda
@mock_iam
@mock_s3
def test_assign_bucket_update_event_triggers_runs_associated_lambda_when_bucket_gets_files_added():
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
    response = create.assign_bucket_update_event_triggers(
        bucket_name='ingest-bucket',
        lambda_arn=deploy.lambda_arns['customLambda'],
        bucket_folders=["folder"])
    with open("deployment/__tests__/test_data/lambda2/main.py", "rb") as file:
        create.s3.upload_fileobj(file, 'ingest-bucket','folder/new_file.py')
    log_group = '/aws/lambda/customLambda'
    log_client = boto3.client('logs')
    response = log_client.describe_log_streams(
        logGroupName=log_group
    )
    assert 'logStreams' in response
    assert len(response['logStreams']) > 0
    log_arn = response['logStreams'][0]['arn']
    
    