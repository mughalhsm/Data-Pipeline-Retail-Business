import os
import shutil
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import zipfile


class Create_resources():
    errors = []

    def __init__(self):
        """Initialise the create resources class with the s3 connection started"""
        self.create_aws_connection()

    def create_aws_connection(self):
        """Create the s3 client, using secrets obtained from github secrets"""
        try:
            self.s3 = boto3.client('s3',
                                   region_name='us-east-1')
        except ClientError as ce:
            error = 'Client Error :' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)
            raise ce
        except AttributeError as ae:
            error = "Failed to find attributes 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'"
            print(error)
            self.errors.append(error)
        except Exception as e:
            print(e)
            self.errors.append(e)

    def create_s3_bucket(self, bucket_name: str):
        """Create a bucket of passed name if the name is valid"""
        try:
            self.s3.create_bucket(Bucket=bucket_name)
        except ClientError as ce:
            error = 'Client Error : ' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)

    def assign_bucket_update_event_triggers(self, bucket_name: str, lambda_arn: str, bucket_folders: list):
        """Trigger the appropriate lambda function when a bucket folder has new files added"""
        if lambda_arn==None or bucket_name==None:
            print(f'Incomplete parameters for creating trigger')
            return
        notification_config = {
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': lambda_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': []
                        }
                    }
                }
            ]
        }
        for folder in bucket_folders:
            notification_config['LambdaFunctionConfigurations'][0]['Filter']['Key']['FilterRules'].append(
                    {
                        'Name': 'prefix',
                        'Value': folder
                    }
                )
        print(f"Applying configurations : {notification_config}")
        print(f"For {bucket_name}")
        try:
            response = self.s3.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification_config
            )
            return response
        except ClientError as ce:
            error = 'Client Error : ' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)
            raise ce

    def upload_lambda_function_code(self, folder_path: str, code_bucket: str, lambda_name: str='lambda', prerequisite_zip:str=None):
        """Using a folder path, lambda name, and destination code bucket, zip the lambda into an archive and upload it to aws s3 bucket"""
        try:
            zip_directory(folder_path,zip_name=f'{lambda_name}.zip',base_zip=prerequisite_zip)
            with open(f'{lambda_name}.zip', "rb") as file:
                self.s3.upload_fileobj(file, code_bucket, f'{lambda_name}.zip')
        except ClientError as nb:
            print(
                f"Bucket does not exist. Upload of {lambda_name} to {code_bucket} failed")
        except Exception as e:
            raise e


def zip_directory(folder_path: str,zip_name:str="lambda.zip",write_type:str="w",base_zip:str=None):
    """Create a zip file, where the contents are at the top level where they would be with respect for their folder's path"""
    """If a zip file of prerequisites is used as a baseline, copy it to be of the appropriate name"""
    if base_zip != None:
        write_type='a'
        shutil.copy(base_zip,zip_name)
    zip_file = zipfile.ZipFile(zip_name, write_type, zipfile.ZIP_DEFLATED)
    zip_walk(folder_path, zip_file, "")


def zip_walk(folder_path: str, zip_file: zipfile.ZipFile, target_subfolder: str = ""):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            zip_file.write(os.path.join(root, file),
                           target_subfolder+os.path.relpath(path=os.path.join(root, file),
                                                            start=os.path.join(".", folder_path)))
