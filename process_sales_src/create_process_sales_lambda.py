import boto3
import time
from botocore.exceptions import ClientError
import time
import json

def create_lambda():
    
    iam = boto3.client('iam')
    lambda_client = boto3.client('lambda')


    try:
        role_name = 'lambda-process-sales-role'
        assume_role_policy_document = '{"Version": "2012-10-17","Statement": \
            [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},\
                "Action": "sts:AssumeRole"}]}'
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("User already exists")
        else:
            print("Unexpected error in role creation")

    # Create the role
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument = assume_role_policy_document
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("User already exists")
        else:
            print("Unexpected error in role creation")

    try:
        iam_create_role_response_ARN = response['Role']['Arn'] 
    except NameError as ne:
        print('Name Error, role most likely exists already')
        get_role_response=iam.get_role(RoleName=role_name)
        iam_create_role_response_ARN = get_role_response['Role']['Arn']
        

    time.sleep(1)


    s3readpolicyARN = iam.create_policy(
        PolicyName='s3readpolicyProcessData',
        PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    "arn:aws:s3:::bosch-deploy-23-12-22-v2-ingest-bucket",
                    "arn:aws:s3:::bosch-deploy-23-12-22-v2-code-bucket"
                ]
            }
        ]
    }),
        Description='Permission to read the s3 code bucket and the s3 ingest bucket'
    )['Policy']['Arn']


    s3writepolicyARN = iam.create_policy(
        PolicyName='s3writepolicyProcessData',
        PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::bosch-deploy-23-12-22-v2-processed-bucket"
                ]
            }
        ]
    }),
        Description='Permission to read the s3 code bucket and the s3 ingest bucket'
    )['Policy']['Arn']


    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=s3readpolicyARN
    )

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=s3writepolicyARN
    )


    CWPolicyDoc = '{ "Version": "2012-10-17", "Statement": \
        [ { "Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "arn:aws:logs:us-east-1::*" }, \
            { "Effect": "Allow", "Action": [ "logs:CreateLogStream", "logs:PutLogEvents" ], \
                "Resource": "arn:aws:logs:us-east-1::log-group:/aws/lambda/Process-Sales-Function:*" } ] }' 
    time.sleep(2)
    try:
        cloudwatchPolicyArn = iam.create_policy(
            PolicyName=f'cloudwatch_policy-{role_name}',
            PolicyDocument=CWPolicyDoc,
            Description='cloudwatch policy for the data processing function for totesys sales schema'
        )['Policy']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("User already exists")
            

    ## above succesfully creates the policy for us-east-1 to create log group for specific function


    time.sleep(1)
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=cloudwatchPolicyArn
        )
    except Exception as e:
        print(e)
    


    time.sleep(4) ##Lets things catch up, was having the problem of trying to reference the arn in the role section below
    ## but kept coming back saying the role wasnt defined


    # Create the Lambda function
    lambda_client.create_function(
        FunctionName='Process-Sales-Function',
        Runtime='python3.9',
        Role=iam_create_role_response_ARN,
        Handler='process_sales_Lambda.sales_handler',
        Code={'S3Bucket': 'bosch-deploy-23-12-22-v2-code-bucket',
        'S3Key':'bosch-deploy-23-12-22-v2-process_sales.zip'},
        Description='This function runs the code from the process sales file to process the ingested data and write it to the processed bucket',
        Timeout=300,
        MemorySize=128
    )




