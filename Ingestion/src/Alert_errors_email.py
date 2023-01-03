import boto3
from botocore.exceptions import ClientError
import os


# Create a metric filter that spots the "ERROR" event
def put_metric_filter_func(logGroupName, filterName, filterPattern, metricTransformations):
    """Attempts to put metric filter after verifiying that log group exists"""
    cloudwatch_logs = boto3.client("logs",region_name='us-east-1')
    try:
        print('checking if log group exists')
        log_describe_response = cloudwatch_logs.describe_log_groups()
        for logGroup in log_describe_response['logGroups']:
            if logGroup['logGroupName'] == logGroupName:
                print('found log group')
                continue
    except ClientError as ce:
        print('Error', ce)
        quit(ce)
    print('trying to put metric filter')
    try:
        put_metric_response = cloudwatch_logs.put_metric_filter(
            logGroupName=logGroupName,
            filterName=filterName,
            filterPattern=filterPattern,
            metricTransformations=metricTransformations
        )
    except ClientError as ce:
        print('Error', ce)
        quit(ce.response['Error']['Code'])
    except TypeError as te:
        print('Error', te)
        quit(te.response['Error']['Code'])
    return put_metric_response


###Creating a topic and subscribing to it 
def create_sns_topic():
    sns = boto3.client("sns",region_name='us-east-1')
    try:
        print('trying to create sns topic')
        create_response = sns.create_topic(
            Name="errors-topic"
        )
        createdErr_topic_arn = create_response["TopicArn"]
        # print(createdErr_topic_arn)
        return createdErr_topic_arn
    except ClientError as ce:
        print('Error', ce)
        quit()



def subscribe_by_email(createdErr_topic_arn):
    sns = boto3.client("sns",region_name='us-east-1')
    try:
        print('trying to subscribe to email')
        sub_response = sns.subscribe(
            TopicArn=createdErr_topic_arn,
            Protocol="email",
            Endpoint="ceesjd@hotmail.co.uk"
        )
    except ClientError as ce:
        print('Error', ce)
        quit()



##Adds metric alarm, testing against the 1 given by the filter, if >= then 
##alarmactions = topic_arn which sends me an email
def put_metric_alarm(createdErr_topic_arn):
    cloudwatch = boto3.client('cloudwatch',region_name='us-east-1')
    try:
        print('trying to put metric alarm')
        metric_alarm_response = cloudwatch.put_metric_alarm(
        AlarmName="errors-alarm",
        AlarmDescription="Alarm when there are errors in the logs",
        ActionsEnabled=True,
        MetricName="errors-metric",
        Namespace="Testing_for_errors_Lambda_With_Metric_Filter",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[createdErr_topic_arn]
    )
    except ClientError as ce:
        print('Error', ce)
        quit()


def sub_list():
    sns = boto3.client("sns",region_name='us-east-1')
    try:
        print('showing subscribers')
        subscriber_list=sns.list_subscriptions()
        print('Subscribers: ', subscriber_list['Subscriptions'])
    except ClientError as ce:
        print('Error', ce)
        quit()


## thoughts on how i could make this better, make it user input based and f strings
## then easier to add people to a certain topic or something like that

def main():
    put_metric_filter_func(logGroupName="/aws/lambda/Ingestion_Function5", filterName="errors-filter", filterPattern="ERROR", metricTransformations=[
                {
                    "metricValue": "1",
                    "metricNamespace": "Testing_for_errors_Lambda_With_Metric_Filter",
                    "metricName": "errors-metric",
                    "defaultValue": 0
                }
            ])
    createdErr_topic_arn = create_sns_topic()
    print('this is your topic arn', createdErr_topic_arn)
    subscribe_by_email(createdErr_topic_arn)
    put_metric_alarm(createdErr_topic_arn=createdErr_topic_arn)

if __name__ == "__main__":
    main()