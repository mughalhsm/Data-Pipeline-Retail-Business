import boto3
lambda_client = boto3.client('lambda')
from botocore.exceptions import ClientError
import logging

try:
    response4 = lambda_client.update_function_code(FunctionName='Ingestion_Function5', ZipFile=open('UploadPackage55.zip', 'rb').read())
    print('Updated lambda code')
except ClientError as e:  
    logging.error(e)
except FileNotFoundError as fe:
    print('File not found, missed something in setup?', fe)
