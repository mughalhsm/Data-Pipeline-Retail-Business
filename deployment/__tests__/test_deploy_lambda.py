from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.create_buckets import Create_resources
from deployment.src.assign_iam import Assign_iam
from moto import mock_lambda, mock_iam, mock_s3
from unittest.mock import patch
import pytest

@mock_s3
@mock_iam
@mock_lambda
def test_create_lambda_successfully_deploys_when_zip_is_available():
    create = Create_resources()
    create.create_s3_bucket("code-bucket")
    create.upload_lambda_function_code(code_bucket="code-bucket",folder_path="deployment/__tests__/test_data/lambda1",lambda_name="customLambda")
    permit = Assign_iam()
    permit.create_lambda_role(role_name="test-role")
    permit.attach_execution_role(role_name='test-role')
    permit.create_cloudwatch_logging_policy(lambda_name='test-lambda')
    permit.attach_custom_policy(role_name='test-role',policy='cloudwatch-policy-test-lambda')
    role_arn = permit.role_arns['test-role']
    deploy = Deploy_lambdas()
    result = deploy.create_lambda(lambda_name="test-lambda",code_bucket="code-bucket",role_arn=role_arn,zip_file="customLambda.zip",handler_name="lambda_handler")
    assert result['ResponseMetadata']['HTTPStatusCode'] == 201
    assert result['FunctionName'] == 'test-lambda'
    assert 'FunctionArn' in result