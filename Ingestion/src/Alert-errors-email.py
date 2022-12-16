import boto3
from botocore.exceptions import ClientError

cloudwatch_logs = boto3.client("logs")
sns = boto3.client("sns")
cloudwatch = boto3.client('cloudwatch')

# Create a metric filter that spots the "ERROR" event
def put_metric_filter():
    try:
        put_metric_response = cloudwatch_logs.put_metric_filter(
            logGroupName="/aws/lambda/Ingestion_Function5",
            filterName="errors-filter",
            filterPattern="ERROR",
            metricTransformations=[
                {
                    "metricValue": "1",
                    "metricNamespace": "Testing_for_errors_Lambda_With_Metric_Filter",
                    "metricName": "errors-metric",
                    "defaultValue": 0
                }
            ]
        )
    except ClientError as ce:
        print(ce)
        quit()
    # print (put_metric_response)
    return put_metric_response


put_metric_filter()
###Creating a topic and subscribing to it 
def create_sns_topic():
    create_response = sns.create_topic(
        Name="errors-topic"
    )
    createdErr_topic_arn = create_response["TopicArn"]
    # print(createdErr_topic_arn)
    return createdErr_topic_arn


createdErr_topic_arn = create_sns_topic()

def subscribe_by_email():
    sub_response = sns.subscribe(
        TopicArn=createdErr_topic_arn,
        Protocol="email",
        Endpoint="ceesjdu@gmail.com"
    )
    # print(sub_response)

subscribe_by_email()

##Adds metric alarm, testing against the 1 given by the filter, if >= then 
##alarmactions = topic_arn which sends me an email
def put_metric_alarm(createdErr_topic_arn):
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
    # print(metric_alarm_response)


put_metric_alarm(createdErr_topic_arn=createdErr_topic_arn)

subscriber_list=sns.list_subscriptions()
print('Subscribers: ', subscriber_list['Subscriptions'])


## thoughts on how i could make this better, make it user input based and f strings
## then easier to add people to a certain topic or something like that


