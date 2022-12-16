#!/bin/bash
# Rule config

set -u
set -e 

echo 'Creating rule'

FUNCTION_NAME=Ingestion_Function5
AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq .Account | tr -d '"')
AWS_REGION='us-east-1'

LAMBDA_ARN=arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:${FUNCTION_NAME}
RULE_NAME=${FUNCTION_NAME}-SCHEDULE
SCHEDULE='rate(10 minutes)'
RULE_ARN=$(aws events put-rule --name ${RULE_NAME} --schedule-expression "${SCHEDULE}" | jq .RuleArn | tr -d '"')

# Add permission to allow function to be invoked by scheduler
echo 'Adding Rule notification and permission'
PERMISSION=$(aws lambda add-permission --function-name ${FUNCTION_NAME} --principal events.amazonaws.com \
--statement-id rate_invoke --action "lambda:InvokeFunction" \
--source-arn ${RULE_ARN} \
--source-account ${AWS_ACCOUNT_ID})

RULE_TARGETS=$(aws events put-targets --rule ${RULE_NAME} --targets "Id"="${FUNCTION_NAME}","Arn"="${LAMBDA_ARN}")

set +u 
set +e 
