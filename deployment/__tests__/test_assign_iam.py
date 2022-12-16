from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.assign_iam import Assign_iam, create_cloudwatch_policy_json, create_s3_access_policy_json
from moto import mock_iam
from unittest.mock import patch
import pytest



@patch('deployment.src.create_buckets.os')
@mock_iam
def test_create_resource_creates_connection_on_instance_creation(os):
    """Mocking environment variables to mimic github secrets and mocking boto3, test that iam connection exists"""
    os.environ.return_value = {'GITHUB_TOKEN': {
        'AWS_ACCESS_KEY': "temp",
        'AWS_SECRET_KEY': "tempSecret"}}
    permissions = Assign_iam()
    assert permissions.iam != None

def test_create_cloudwatch_policy_json_returns_string_of_appropriate_format_with_passed_lambda_name():
    expected = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "arn:aws:logs:us-east-1::*"}, {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "arn:aws:logs:us-east-1::log-group:/aws/lambda/testlambda:*"}]}'
    result = create_cloudwatch_policy_json('testlambda')
    assert result == expected

@mock_iam
def test_create_role_of_passed_name():
    
    permissions = Assign_iam()
    result = permissions.create_lambda_role("test_role")
    assert "Arn" in result['Role']
    assert result['Role']['AssumeRolePolicyDocument'] == {'Statement': [{'Action': 'sts:AssumeRole', 'Effect': 'Allow', 'Principal': {'Service': 'lambda.amazonaws.com'}}], 'Version': '2012-10-17'}

@mock_iam
def test_attach_execution_policy_to_role_of_passed_name():    
    permissions = Assign_iam()
    permissions.create_lambda_role("test_role")
    result = permissions.attach_execution_role("test_role")
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

def test_create_s3_access_policy_json_returns_string_of_appropriate_format_and_no_Statements_when_none_are_requested():
    expected = '{"Version": "2012-10-17", "Statement": []}'
    result = create_s3_access_policy_json('test-bucket')
    assert result == expected


def test_create_s3_access_policy_json_returns_string_of_appropriate_format_and_any_of_the_requested_permissions():
    expected = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::test-bucket"]}]}'
    result = create_s3_access_policy_json('test-bucket',list=True)
    assert result == expected
    expected = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::test-bucket/*"]}]}'
    result = create_s3_access_policy_json('test-bucket',get=True)
    assert result == expected
    expected = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": ["arn:aws:s3:::test-bucket/*"]}]}'
    result = create_s3_access_policy_json('test-bucket',put=True)
    assert result == expected
    expected = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::test-bucket"]}, {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::test-bucket/*"]}, {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": ["arn:aws:s3:::test-bucket/*"]}]}'
    result = create_s3_access_policy_json('test-bucket',list=True,get=True,put=True)
    assert result == expected

@mock_iam
def test_create_cloudwatch_logging_policy_creates_logging_policy_for_passed_lambda():
    permissions = Assign_iam()
    result = permissions.create_cloudwatch_logging_policy("test-lambda")
    assert result['Policy']['PolicyName'] == 'cloudwatch-policy-test-lambda'
    assert 'Arn' in result['Policy']
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

@mock_iam
def test_create_s3_ingest_read_policy_creates_a_policy_for_passed_lambda_to_read_the_passed_bucket():
    permissions = Assign_iam()
    result = permissions.create_s3_read_write_policy("test-lambda","test-bucket")
    print(result)
    assert result['Policy']['PolicyName'] == 's3-read-test-bucket-test-lambda'
    assert 'Arn' in result['Policy']
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

@mock_iam
def test_attach_custom_policy_adds_the_policy_to_the_appropriate_role():
    permissions = Assign_iam()
    result = permissions.create_s3_read_write_policy("test-lambda","test-bucket")
    permissions.create_lambda_role(role_name='test-role')
    result = permissions.attach_custom_policy(role_name='test-role',policy='s3-read-test-bucket-test-lambda')
    print(result)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = permissions.iam.list_attached_role_policies(RoleName='test-role')
    assert 's3-read-test-bucket-test-lambda' in [policy['PolicyName'] for policy in result['AttachedPolicies']]
