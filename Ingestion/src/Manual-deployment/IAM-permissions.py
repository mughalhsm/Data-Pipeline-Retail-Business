import boto3
from botocore.exceptions import ClientError
iam = boto3.client('iam')

IAM_ALLOW_ALL='{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "*","Resource": "*"}]}' ## bad security practice 
role_name = 'lambda-ingestion-execution-role19'

try:
    response_of_create_policy = iam.create_policy(
        PolicyName=f'iam-ALLOW-ALL',
        PolicyDocument=IAM_ALLOW_ALL,
        Description='allow all IAM secret manager access' ## description is specific
    )
    arn_of_allow_all=response_of_create_policy['Arn']
except ClientError as ce:
    print(ce)
#     iam_create_role_response_ARN = response_of_create_policy['Arn']

# print(response_of_create_policy)

# arn_of_allow_all=response_of_create_policy['Arn']

iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn=iam_create_role_response_ARN
)

