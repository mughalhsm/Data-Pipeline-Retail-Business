import boto3
# Script that provides event bridge scheduler for specific lambda function
# Set up the client for the AWS Events service
def main():
    events_client = boto3.client('events')
    lambda_client = boto3.client('lambda')

    # Rule config
    FUNCTION_NAME = 'Ingestion_Function5'
    AWS_REGION = 'us-east-1'

    # Get the AWS account ID
    sts_client = boto3.client('sts')
    caller_identity = sts_client.get_caller_identity()
    AWS_ACCOUNT_ID = caller_identity['Account']

    # Create the ARN for the Lambda function, e.g - arn:aws:lambda:us-east-1:448064563446:function:Ingestion_Function5
    LAMBDA_ARN = f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_ID}:function:{FUNCTION_NAME}"


    # Set the schedule expression for the rule and create rule name
    SCHEDULE = 'rate(2 minutes)'
    RULE_NAME = f"{FUNCTION_NAME}-scheduler"

    # Create the rule
    response_of_put_rule = events_client.put_rule(
        Name=RULE_NAME,
        ScheduleExpression=SCHEDULE,
    )

    # Get the ARN of the created rule
    RULE_ARN = response_of_put_rule['RuleArn']

    # Add permission to allow the function to be invoked by the scheduler
    lambda_client.add_permission(
        FunctionName=FUNCTION_NAME,
        Principal='events.amazonaws.com',
        StatementId='rate_invoke_2min',
        Action='lambda:InvokeFunction',
        SourceArn=RULE_ARN,
        SourceAccount=AWS_ACCOUNT_ID
    )

    # Add the target for the rule
    events_client.put_targets(
        Rule=RULE_NAME,
        Targets=[
            {
                'Id': FUNCTION_NAME,
                'Arn': LAMBDA_ARN
            },
        ]
    )

if __name__ == "__main__":
    main()


