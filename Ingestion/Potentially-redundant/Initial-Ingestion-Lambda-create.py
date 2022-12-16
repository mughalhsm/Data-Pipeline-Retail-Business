import boto3
import time
##This file sets all the policies and creates the lambda function.
## Creates basic execution role and cloudwatch for a lambda, and gives the zip package
## need to run a zip before this that takes everything then

# Create an IAM client
iam = boto3.client('iam')

# Set the name of the role and the assume role policy document
role_name = 'lambda-ingestion-execution-role19'
assume_role_policy_document = '{"Version": "2012-10-17","Statement": \
     [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},\
         "Action": "sts:AssumeRole"}]}'

# Create the role
response = iam.create_role(
    RoleName=role_name,
    AssumeRolePolicyDocument = assume_role_policy_document
)

iam_create_role_response_ARN = response['Role']['Arn'] 

time.sleep(1)

iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole' ## this is fine as literal
)

CWPolicyDoc = '{ "Version": "2012-10-17", "Statement": \
     [ { "Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "arn:aws:logs:us-east-1::*" }, \
         { "Effect": "Allow", "Action": [ "logs:CreateLogStream", "logs:PutLogEvents" ], \
             "Resource": "arn:aws:logs:us-east-1::log-group:/aws/lambda/Ingestion_Function4:*" } ] }' ## Would have to change this for specific resource names i think

time.sleep(2)
response_of_create_policy = iam.create_policy(
    PolicyName=f'cloudwatch_policy-{role_name}',
    PolicyDocument=CWPolicyDoc,
    Description='cloudwatch policy for the ingestion function ((((5))))' ## description is specific
)
## above succesfully creates the policy for us-east-1 to create log group for specific function


time.sleep(1)
iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn=response_of_create_policy['Policy']['Arn'] ## attaches the cloudwatch policy we made above
)


time.sleep(30) ##Lets things catch up, was having the problem of trying to reference the arn in the role section below
## but kept coming back saying the role wasnt defined
lambda_client = boto3.client('lambda')


# Create the Lambda function
lambda_client.create_function(
    FunctionName='Ingestion_Function5',
    Runtime='python3.9',
    Role=iam_create_role_response_ARN,
    Handler='index.lambda_handler',
    Code={
        'ZipFile': open('LambdaScript/Connection.zip', 'rb').read() ## Need to change this to run prezipped deployment package
    },
    Description='This function takes sample code from a bucket and runs it',
    Timeout=300,
    MemorySize=128
)











## so as far i have the basic lambda, its just not being used yet, so need to automate it, also cloudwatch, but 
## can add logging now.