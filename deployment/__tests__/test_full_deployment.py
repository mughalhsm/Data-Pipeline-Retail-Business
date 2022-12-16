import deployment.run_bucket_creation as run_buckets
import deployment.run_lambda_creation as run_lambda
from moto import mock_s3, mock_iam, mock_lambda, mock_logs, mock_events


@mock_logs
@mock_events
@mock_lambda
@mock_iam
@mock_s3
def test_full_deployment():
    try:
        run_buckets.create_buckets()
        run_lambda.deploy_lambdas()
    except Exception as e:
        print(e.response['Message'])
        assert False