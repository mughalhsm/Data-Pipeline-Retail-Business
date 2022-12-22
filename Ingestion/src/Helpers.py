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


def put_into_bucket(bucket_name, table_name,
    increment, dataframe_as_csv):

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.put_object(Bucket=bucket_name, 
    Key=f"TableName/{table_name}/RunNum:{increment}.csv", 
    Body=dataframe_as_csv),



def delete_last_run_num_object(bucket_name, prefix):
    '''If ingestion fails, and increment has already happened, then this will
    clean up the increment file'''
    print('Returning last object name for specific prefix...')
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    print(latest['Key'], '<- Removing...')
    s3.delete_object(Bucket=bucket_name, Key=latest['Key'])

##Validates that what is being passed into sql query is correct
def table_name_checker(table_name):
    table_names=['counterparty',
                'currency',
                'department',
                'design',
                'staff',
                'sales_order',
                'address',
                'payment',
                'purchase_order',
                'payment_type',
                'transaction']
    if table_name in table_names:
        return True
    return False


   
   ## ## ##
def delete_TESTFUNC_last_run_num_object(bucket_name):
    '''Removed prefix for unit testing, works the same tested in console '''
    print('Returning last object name for specific prefix...')
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    print(latest['Key'], '<- Removing...')
    s3.delete_object(Bucket=bucket_name, Key=latest['Key'])
   ## ## ##

