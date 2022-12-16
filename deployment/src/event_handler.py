import os
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


class Create_events():
    errors = []
    event_arns = {}

    def __init__(self):
        """Initialise the create resources class with the s3 connection started"""
        self.create_aws_connection()

    def create_aws_connection(self):
        """Create the events client, using secrets obtained from github secrets"""
        try:
            if 'GITHUB_TOKEN' in os.environ:
                github_secrets: dict = os.environ['GITHUB_TOKEN']
                os.environ['AWS_ACCESS_KEY_ID'] = github_secrets['AWS_ACCESS_KEY']
                os.environ['AWS_SECRET_ACCESS_KEY'] = github_secrets['AWS_SECRET_KEY']

            self.events = boto3.client('events',
                                       region_name='us-east-1')
        except ClientError as ce:
            error = 'Client Error :' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)
        except AttributeError as ae:
            error = "Failed to find attributes 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY' on key 'GITHUB_TOKEN'"
            print(error)
            self.errors.append(error)
        except KeyError as ke:
            error = "Failed to find keys 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY' on key 'GITHUB_TOKEN'"
            print(error)
            self.errors.append(error)
        except Exception as e:
            print(e)
            self.errors.append(e)

    def create_schedule_event(self, schedule_name: str, minute_count: str, *, period: str = "minutes", state: bool = True):
        """Create a schedule event of passed name and duration"""
        try:
            response = self.events.put_rule(
                Name=schedule_name,
                ScheduleExpression=f'rate({minute_count} {period})',
                State='ENABLED' if state else "DISABLED"
            )
            self.event_arns[schedule_name] = response['RuleArn']
            return response
        except TypeError as ce:
            error = 'Client Error : ' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)

    def assign_event_target(self, schedule_name: str, target_arn: str):
        """For the passed in lambda arn, assign a rule to the lambda"""
        try:
            response = self.events.put_targets(
                Rule=schedule_name,
                Targets=[
                    {
                        'Arn': target_arn,
                        'Id': 'scheduled_trigger'
                    }
                ]
            )
            return response
        except ClientError as ce:
            error = 'Client Error : ' + ce.response['Error']['Message']
            print(error)
            self.errors.append(error)
