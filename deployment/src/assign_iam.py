"""Class to handle a connection to the aws iam service, to create and attach policies and roles as needed.

Creating an instance of the class creates a client connection to the aws iam service using boto3.
Once this connection has been made, the methods within the class use the created client to create policies and roles.
When a policy or role is successfully made, the arn is stored in a dictionary such that they can be referenced by name.
If a role or policy fails to be created due to a role or policy of that name already existing, the arn of that existing 
policy or role is obtained under the assumption it had already been created for the same purpose.

Typical usage : 
iam = Assign_Iam()
iam.create_s3_read_write_policy(bucket=bucket_name, lambda_name=my_lambda_name, read=True, write=True)
permit.create_lambda_role(role_name=process_purchases_role)
permit.attach_custom_policy(role_name=process_purchases_role, policy=f's3-read-write-{bucket_name}-{lambda_name}')
"""
import os
import boto3
from botocore.exceptions import ClientError
import json
import time


class Assign_iam():
    """Class to handle creation of a connection to the aws iam service, store formed arns, and handle iam behaviour"""
    aws_lambda_execution_policy = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    role_arns = {}
    policy_arns = {}

    def __init__(self):
        self.create_aws_connection()

    def create_aws_connection(self):
        """Create the lambda client, using secrets obtained from github secrets"""
        try:
            self.iam = boto3.client('iam', region_name='us-east-1')
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

    def create_lambda_role(self, role_name: str):
        """Sets up role of passed name
        
        Uses the iam client to create a role with the ability of a 
        lambda function to assume said role, and saves the arn on a
        key of the name in roles.
        If the server is not erroring, but is not returning the role
        information, it will attempt 10 times at second intervals to
        allow the server to process the request
        
        Args: 
            role_name (str): The name of the role to be created
        
        Returns (dict):
            A dict with the server's response of the created role 
            on a key of "Role", for example : 
            {
                "Role": {
                        "RoleName": "role_name",
                        "Arn": "arn::... ...",
                        "AssumeRolePolicyDocument": {...}
                        }
            }
        """
        response = ""
        # ,{"Effect": "Allow","Action": [ "iam:PassRole"],"Resource": ["arn:aws:iam:::*"]}]}'
        lambda_role_document = '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},"Action": "sts:AssumeRole"}]}'
        try:
            response = self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=lambda_role_document
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'{role_name} role already exists, reading from iam')
                responses = self.iam.list_roles()
                response = {
                    'Role': role for role in responses['Roles'] if role_name == role['RoleName']}
        attempt = 0
        while ('Role' not in response or 'RoleName' not in response['Role'] or response['Role']['RoleName'] != role_name) and attempt < 10:
            time.sleep(1)
            responses = self.iam.list_roles()
            response = {
                'Role': role for role in responses['Roles'] if role_name == role['RoleName']}
            attempt += 1
        try:
            self.role_arns[role_name] = response['Role']['Arn']
        except TypeError as te:
            f"Failed to get role appropriately, recieved {response}"
        return response

    def verify_stored_arns(self):
        return

    def attach_custom_policy(self, role_name: str, policy: str):
        """Attaches the past policy by name to the passed role
        
        Uses the iam client to attach the passed policy to the passed
        role, by obtaining the policy arn from its name as stored in
        the dictionary within the class instance
        
        Args:
            role_name (str): The name of the role to attach the policy to
            policy (str): The name of the policy to attach
        
        Returns (dict):
            An empty dictionary with a printed error if the policy is
                not stored within the dictionary
                
            OR
            
            A dict with the server's response to attaching the policy, 
            for example : 
            {
                "ResponseMetadata": {
                        "RequestId": "...",
                        "HTTPStatusCode": 200,
                        ...
                        }
            }
        """
        if not policy in self.policy_arns:
            print(
                f'Failed to attach {policy} to {role_name} - policy arn not found in {self.policy_arns}')
            return {}
        arn = self.policy_arns[policy]
        response = self.iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=arn
        )
        return response

    def attach_execution_role(self, role_name: str):
        """Attaches the AWS lambda execution policy to the passed role
        
        Uses the iam client to attach the lambda execution policy to the
        passed role, by obtaining the policy arn from its name as stored in
        the dictionary within the class instance
        
        Args:
            role_name (str): The name of the role to attach the policy to
        
        Returns (dict):
            A dict with the server's response to attaching the policy, 
            for example : 
            {
                "ResponseMetadata": {
                        "RequestId": "...",
                        "HTTPStatusCode": 200,
                        ...
                        }
            }"""
        response = self.iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        return response

    def create_cloudwatch_logging_policy(self, lambda_name: str):
        """Creates a cloudwatch policy for having access to the lambda's logger
        
        Takes the lambda name and creates a cloudwatch policy of form
        cloudwatch-policy-{lambda_name}, saving the arn as it is created to
        the class instance's policy dictionary.
        If the policy already exists, obtains the information from the policy
        list, and saves the arn, returning the appropriate dict
        
        Args:
            lambda_name (str): The name of the lambda to create a policy for
        
        Return (dict):
            {
                "Policy": {
                        "PolicyName": "cloudwatch-policy-{lambda_name}",
                        "Arn": "arn::... ..."
                        }
            }
        """
        response = ""
        policy_name = f'cloudwatch-policy-{lambda_name}'
        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=create_cloudwatch_policy_json(lambda_name),
                Description=f'Cloudwatch policy for {lambda_name}'
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'{policy_name} policy already exists, reading from iam')
                responses = self.iam.list_policies(Scope='Local')
                response = {
                    'Policy': policy for policy in responses['Policies'] if policy['PolicyName'] == policy_name}

        self.policy_arns[f'cloudwatch-policy-{lambda_name}'] = response['Policy']['Arn']
        return response

    def create_s3_read_write_policy(self, lambda_name: str, bucket: str, read: bool = True, write: bool = False):
        """Creates a policy for reading, and/or writing from the given bucket, and saves the arn on a key of the name in policies
        
        Takes the lambda name and creates an s3 access policy of form
        s3-{name_modifier}-{bucket}-{lambda_name}, saving the arn as it is created to
        the class instance's policy dictionary.
        If the policy already exists, obtains the information from the policy
        list, and saves the arn, returning the appropriate dict
        
        Args:
            lambda_name (str): The name of the lambda for which to create a policy
            bucket (str): The name of the bucket for which to give permissions
            read (bool): A boolean for if the lambda should have read access
            write (bool): A boolean for if the lambda should have write/put access
        
        Return (dict):
            {
                "Policy": {
                        "PolicyName": "s3-{name_modifier}-{bucket}-{lambda_name}",
                        "Arn": "arn::... ..."
                        }
            }
        """
        name_modifier = "read" if not write else "read-write"
        policy_name = f's3-{name_modifier}-{bucket}-{lambda_name}'
        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=create_s3_access_policy_json(
                    bucket, list=read, get=read, put=write),
                Description=f'Read the ingest bucket policy policy for {lambda_name}'
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'{policy_name} policy already exists, reading from iam')
                responses = self.iam.list_policies(Scope='Local')
                response = {
                    'Policy': policy for policy in responses['Policies'] if policy['PolicyName'] == policy_name}
        self.policy_arns[policy_name] = response['Policy']['Arn']
        return response


def create_cloudwatch_policy_json(lambda_name: str):
    """Creates a cloudwatch policy for a passed lambda
    
    Creates a policy document for creating a log group, a log stream,
    and to put log events at a resource location associated with the
    lambda name

    Args:
        lambda_name (str): The name of the lambda to be given permission

    Returns:
        string: String cast of the cloudwatch policy json 
    """
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
                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": f"arn:aws:logs:us-east-1::log-group:/aws/lambda/{lambda_name}:*"
            }
        ]
    }
    return json.dumps(cloudwatch_log_policy)


def create_s3_access_policy_json(bucket: str, list: bool = False, get: bool = False, put: bool = False):
    """Creates a policy document for access to a given bucket, and only the required action permissions
        
    Creates a policy document for accessing the passed bucket, with
    options for the specific level of permissions it should include

    Args:
        bucket (str): The name of the lambda to be given permission
        list (bool): If the policy should include list permissions
        get (bool): If the policy should include get permissions
        put (bool): If the policy should include put permissions

    Returns:
        string: String cast of the access policy json 
    """
    policy_document = {
        "Version": "2012-10-17",
        "Statement": []
    }
    if list:
        policy_document["Statement"].append({
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket}"
            ]
        })
    if get:
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
    if put:
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
