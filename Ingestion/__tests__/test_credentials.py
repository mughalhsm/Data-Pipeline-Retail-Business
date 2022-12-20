import boto3
import json
import pytest
from moto import mock_secretsmanager
from src.cred import get_credentials
from botocore.exceptions import ClientError
import unittest

@pytest.fixture
def secrets_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")



def test_that_returns_correct_credentials_from_a_secret(secrets_client):
    #Create a secret in the mock secrets manager service
    secrets_client.create_secret(
        Name="totesys_credentials",
        SecretString='{"user": "test_user", "password": "test_pass", "host": "test_host"}'
    )

    #Call the get_credentials() function with the secret name
    username, passw, host = get_credentials('totesys_credentials')

    #Assert that the function returns the expected credentials
    assert username == "test_user"
    assert passw == "test_pass"
    assert host == "test_host"        



def test_that_entering_wrong_credentials_name_raises_error(secrets_client):
    secrets_client.create_secret(
        Name="totesys_credentials",
        SecretString='{"user": "test_user", "password": "test_pass", "host": "test_host"}'
    )

    with pytest.raises(UnboundLocalError):
        get_credentials('totsys_credentials')



