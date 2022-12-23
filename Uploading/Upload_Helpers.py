import boto3
from botocore.exceptions import ClientError
import json

def get_credentials(Secret_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )
    try:
        secret = client.get_secret_value(
                SecretId=Secret_name
    )
    except ClientError as ce:
        print('ERROR, check if correct secret name.' , ce.response['Error']['Code'])
        raise ClientError(operation_name='ResourceNotFound', error_response={
            'Error': {
                'Code': 'ResourceNotFound',
                'Message': 'CHECK IF CREDENTIALS ARE CORRECT'
            }
        }) ## Having to force it to raise a ClientError??
   
    secret_dict = json.loads(secret['SecretString'])

    username = secret_dict['user']
    passw = secret_dict['password']
    host = secret_dict['host']

    return username, passw, host