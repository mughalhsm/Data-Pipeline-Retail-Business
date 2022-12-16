import os
import boto3
from botocore.exceptions import ClientError
import json

class Assign_iam():
    aws_lambda_execution_policy = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    role_arns = {}
    policy_arns = {}
    def __init__(self):
        self.create_aws_connection()
        
    def create_aws_connection(self):
        """Create the lambda client, using secrets obtained from github secrets"""
        try:
            self.iam = boto3.client('iam',
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

    def create_lambda_role(self,role_name:str):
        """Sets up role of passed name, with the ability of a lambda function to assume said role, and saves the arn on a key of the name in roles"""
        response = ""
        lambda_role_document = '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},"Action": "sts:AssumeRole"}]}'
        try:
            response = self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument = lambda_role_document
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExistsException':
                print(f'{role_name} role already exists, reading from iam')
                responses = self.iam.list_roles()
                response = {'Role':role for role in responses['Roles']}
        self.role_arns[role_name] = response['Role']['Arn']
        return response
    
    def attach_custom_policy(self, role_name:str,policy:str):
        """Attaches the past policy by name to the passed role"""
        if not policy in self.policy_arns:
            print(f'Failed to attach {policy} to {role_name} - policy arn not found in {self.policy_arns}')
            return ""
        arn = self.policy_arns[policy]
        response = self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=arn
            )
        return response

    def attach_execution_role(self,role_name:str):
        """Attaches the AWS lambda execution policy to the passed role"""
        response = self.iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        return response
    
    def create_cloudwatch_logging_policy(self, lambda_name:str):
        """Creates a cloudwatch policy for having access to the lambda's logger, and saves the arn on a key of the name in policies"""
        response = ""
        policy_name = f'cloudwatch-policy-{lambda_name}'
        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=create_cloudwatch_policy_json(lambda_name),
                Description=f'Cloudwatch policy for {lambda_name}'
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExistsException':
                print(f'{policy_name} policy already exists, reading from iam')
                responses = self.iam.list_policies(Scope='Local')
                response = {'Policy':policy for policy in responses['Policies'] if policy['PolicyName'] == policy_name}
            
        self.policy_arns[f'cloudwatch-policy-{lambda_name}'] = response['Policy']['Arn']
        return response
    
    def create_s3_read_write_policy(self, lambda_name:str, bucket:str,read:bool=True,write:bool=False):
        """Creates a policy for reading, and/or writing from the given bucket, and saves the arn on a key of the name in policies"""
        name_modifier = "read" if not write else "read-write"
        policy_name = f's3-{name_modifier}-{bucket}-{lambda_name}'
        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=create_s3_access_policy_json(bucket,list=read, get=read,put=write),
                Description=f'Read the ingest bucket policy policy for {lambda_name}'
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'{policy_name} policy already exists, reading from iam')
                responses = self.iam.list_policies(Scope='Local')
                response = {'Policy':policy for policy in responses['Policies'] if policy['PolicyName'] == policy_name}
        self.policy_arns[policy_name] = response['Policy']['Arn']
        return response
    


def create_cloudwatch_policy_json(lambda_name:str):
    cloudwatch_log_policy = { 
        "Version": "2012-10-17",
        "Statement": [ 
                {
                    "Effect": "Allow",
                    "Action": "logs:CreateLogGroup", 
                    "Resource": "arn:aws:logs:us-east-1::*" 
                }, 
                {
                    "Effect": "Allow",
                    "Action": [ "logs:CreateLogStream", "logs:PutLogEvents" ], 
                    "Resource": f"arn:aws:logs:us-east-1::log-group:/aws/lambda/{lambda_name}:*" 
                } 
            ] 
        }
    return json.dumps(cloudwatch_log_policy)

def create_s3_access_policy_json(bucket:str,list:bool=False,get:bool=False,put:bool=False):
    """Creates a policy document for access to a given bucket, and only the required action permissions"""
    policy_document = {
        "Version": "2012-10-17",
        "Statement": []
    }
    if list :
        policy_document["Statement"].append({
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}"
                ]
            })
    if get : 
        policy_document["Statement"].append(
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/*"
                ]
            })
    if put : 
        policy_document["Statement"].append(
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/*"
                ]
            })
    return json.dumps(policy_document)