import unittest
from unittest.mock import patch
from src.Ingest_scheduler import getting_caller_id
import boto3
from moto import mock_sts
import pytest
from botocore.exceptions import ClientError


@patch('boto3.client')
def test_getting_caller_id_assuming_server_responds_with_correct_json(mock_client):
    mock_client.getting_caller_id.return_value = {
        'Account': '123456789012',
        'Arn': 'arn:aws:sts::123456789012:assumed-role/role/role-name',
        'UserId': 'AROACLKWSDQRAOEXAMPLE:user-name'
    } 
    identity = mock_client.getting_caller_id()
    print(identity)
    AWS_ACCOUNT_ID = identity['Account']

    assert AWS_ACCOUNT_ID == '123456789012'
