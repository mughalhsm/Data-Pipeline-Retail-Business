import os
import boto3
from botocore.exceptions import ClientError


class Deploy_lambdas():
    lambda_arns = {}

    def __init__(self):
        self.create_aws_connection()

    def create_aws_connection(self):
        """Create the lambda client, using secrets obtained from github secrets"""
        try:
            self.lambda_client = boto3.client('lambda',
                                              region_name='us-east-1')
        except ClientError as ce:
            error = 'Client Error :' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)
        except AttributeError as ae:
            error = "Failed to find attributes 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'"
            print(error)
            self.errors.append(error)
        except Exception as e:
            print(e)
            self.errors.append(e)

    def create_lambda(self, lambda_name: str, code_bucket: str, zip_file: str, role_arn: str, handler_name: str):
        print(
            f"Creating lambda {lambda_name} using {code_bucket} {zip_file} and role arn {role_arn}")
        response = {}
        attempts = 0
        while response == {} and attempts < 10:
            attempts+=1
            try:
                response = self.lambda_client.create_function(
                    FunctionName=lambda_name,
                    Handler=handler_name,
                    Runtime='python3.9',
                    Role=role_arn,
                    Code={
                            'S3Bucket': code_bucket,
                            'S3Key': zip_file
                    }
                )
                self.lambda_arns[lambda_name] = response['FunctionArn']
            except ClientError as ce:
                if ce.response['Error']['Code'] == 'ResourceConflictException':
                    print(
                        f'Lambda {lambda_name} already exists, obtaining information from lambda list')
                    responses = self.lambda_client.list_functions()
                    for funct in responses['Functions']:
                        if lambda_name == funct['FunctionName']:
                            response = funct
                    self.lambda_arns[lambda_name] = response['FunctionArn']
            except Exception as e:
                print(
                    f"Error creating lambda {lambda_name} using {code_bucket} {zip_file} and role arn {role_arn}")
                print(e.response)
                response = e.response
        print(f"Created {response}")
        return response

    def create_lambda_layer(self, layer_name: str, zipfile: str, description: str):
        with open(zipfile, 'rb') as file:
            file_contents = file.read()
            response = self.lambda_client.publish_layer_version(
                LayerName=layer_name,
                Content={'ZipFile': file_contents},
                CompatibleRuntimes=['python3.9'],
                Description=description
            )
        return response
