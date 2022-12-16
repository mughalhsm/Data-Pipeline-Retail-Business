import boto3
import json
from botocore.exceptions import ClientError

##works on the basis that something has been uploaded into secrets manager under the name of 'totesys_credentials'

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
        print('ERROR, check if correct secret name.' , ce)
        quit()

    secret_dict = json.loads(secret['SecretString'])

    username = secret_dict['user']
    passw = secret_dict['password']
    host = secret_dict['host']

    return username, passw, host

