from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.assign_iam import Assign_iam
from deployment.src.create_buckets import Create_resources
from deployment.src.event_handler import Create_events

ingest_lambda_name = "ingest"
process_payments_lambda_name = "process_payments"
process_purchases_lambda_name = "process_purchases"
process_sales_lambda_name = "process_sales"
upload_lambda_name = "upload"

ingest_role = "ingest-role"
process_payments_role = "process-payments-role"
process_purchases_role = "process-purchases-role"
process_sales_role = "process-sales-role"
warehouse_uploader_role = "warehouse-uploader-role"

processed_bucket_name = 'processed-bucket'
ingest_bucket_name = 'ingest-bucket'
code_bucket_name = 'code-bucket'


def deploy_lambdas():
    permit = Assign_iam()
    print("Creating policies")
    create_policies(permit)
    print("Creating roles and attaching policies")
    create_roles(permit)
    deploy = Deploy_lambdas()
    print("Creating ingest lambda")
    create_lambdas(permit, deploy, ingest_lambda_name, ingest_role)
    print("Creating process payments lambda")
    create_lambdas(permit, deploy, process_payments_lambda_name,
                   process_payments_role)
    print("Creating process purchases lambda")
    create_lambdas(permit, deploy, process_purchases_lambda_name,
                   process_purchases_role)
    print("Creating process sales lambda")
    create_lambdas(permit, deploy, process_sales_lambda_name,
                   process_sales_role)
    print("Creating warehouse uploader lambda")
    create_lambdas(permit, deploy, upload_lambda_name, warehouse_uploader_role)
    create = Create_resources()

    print("Assigning triggers to ingest bucket")
    print("1")
    create.assign_bucket_update_event_triggers(
        bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_payments_lambda_name], bucket_folders=['TableName/'])
    print("2")
    create.assign_bucket_update_event_triggers(
        bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_purchases_lambda_name], bucket_folders=['TableName/'])
    print("3")
    create.assign_bucket_update_event_triggers(
        bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_sales_lambda_name], bucket_folders=['TableName/'])
    print("4")
    create.assign_bucket_update_event_triggers(
        bucket_name=processed_bucket_name, lambda_arn=deploy.lambda_arns[upload_lambda_name], bucket_folders=[''])
    print("Creating scheduled trigger")
    event = Create_events()
    event.create_schedule_event(f'schedule-event-{ingest_lambda_name}', '5')
    lambda_arn = deploy.lambda_arns[ingest_lambda_name]
    event.assign_event_target(
        schedule_name=f'schedule-event-{ingest_lambda_name}', target_arn=lambda_arn)


def create_lambdas(permit: Assign_iam, deploy: Deploy_lambdas, lambda_name, role_name):
    deploy.create_lambda(lambda_name=lambda_name, code_bucket=code_bucket_name,
                         role_arn=permit.role_arns[role_name], zip_file=f'{lambda_name}.zip')


def create_roles(permit: Assign_iam):
    permit.create_lambda_role(role_name=ingest_role)
    permit.attach_custom_policy(
        role_name=ingest_role, policy=f"s3-read-{ingest_bucket_name}-{ingest_lambda_name}")
    permit.attach_custom_policy(
        role_name=ingest_role, policy=f"cloudwatch-policy-{ingest_lambda_name}")
    permit.attach_execution_role(role_name=ingest_role)

    permit.create_lambda_role(role_name=process_payments_role)
    permit.attach_custom_policy(role_name=process_payments_role,
                                policy=f"s3-read-{ingest_bucket_name}-{process_payments_lambda_name}")
    permit.attach_custom_policy(role_name=process_payments_role,
                                policy=f"s3-read-write-{processed_bucket_name}-{process_payments_lambda_name}")
    permit.attach_custom_policy(role_name=process_payments_role,
                                policy=f"cloudwatch-policy-{process_payments_lambda_name}")
    permit.attach_execution_role(role_name=process_payments_role)

    permit.create_lambda_role(role_name=process_purchases_role)
    permit.attach_custom_policy(role_name=process_purchases_role,
                                policy=f"s3-read-{ingest_bucket_name}-{process_purchases_lambda_name}")
    permit.attach_custom_policy(role_name=process_purchases_role,
                                policy=f"s3-read-write-{processed_bucket_name}-{process_purchases_lambda_name}")
    permit.attach_custom_policy(role_name=process_purchases_role,
                                policy=f"cloudwatch-policy-{process_purchases_lambda_name}")
    permit.attach_execution_role(role_name=process_payments_role)

    permit.create_lambda_role(role_name=process_sales_role)
    permit.attach_custom_policy(role_name=process_sales_role,
                                policy=f"s3-read-{ingest_bucket_name}-{process_sales_lambda_name}")
    permit.attach_custom_policy(role_name=process_sales_role,
                                policy=f"s3-read-write-{processed_bucket_name}-{process_sales_lambda_name}")
    permit.attach_custom_policy(role_name=process_sales_role,
                                policy=f"cloudwatch-policy-{process_sales_lambda_name}")
    permit.attach_execution_role(role_name=process_sales_role)

    permit.create_lambda_role(role_name=warehouse_uploader_role)
    permit.attach_custom_policy(role_name=process_sales_role,
                                policy=f"s3-read-{processed_bucket_name}-{upload_lambda_name}")
    permit.attach_custom_policy(
        role_name=process_sales_role, policy=f"cloudwatch-policy-{upload_lambda_name}")
    permit.attach_execution_role(role_name=warehouse_uploader_role)


def create_policies(permit: Assign_iam):
    processed_bucket_name = 'processed-bucket'
    ingest_bucket_name = 'ingest-bucket'
    permit.create_cloudwatch_logging_policy(lambda_name=ingest_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=ingest_lambda_name, read=True, write=True)

    permit.create_cloudwatch_logging_policy(
        lambda_name=process_payments_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_payments_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_payments_lambda_name, read=True, write=True)

    permit.create_cloudwatch_logging_policy(
        lambda_name=process_purchases_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_purchases_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_purchases_lambda_name, read=True, write=True)

    permit.create_cloudwatch_logging_policy(
        lambda_name=process_sales_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_sales_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_sales_lambda_name, read=True, write=True)

    permit.create_cloudwatch_logging_policy(lambda_name=upload_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=processed_bucket_name, read=True)


if __name__ == '__main__':
    deploy_lambdas()
