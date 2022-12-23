from deployment.src.deploy_lambdas import Deploy_lambdas
from deployment.src.assign_iam import Assign_iam
from deployment.src.create_buckets import Create_resources
from deployment.src.event_handler import Create_events

deploy_prefix = "bosch-deploy-23-12-22"

ingest_handler_name = f"my_handler"
process_payments_handler_name = ""
process_purchases_handler_name = "lambda_handler"
process_sales_handler_name = ""
upload_handler_name = "lambda_handler"

ingest_lambda_name = f"{deploy_prefix}ingest"
process_payments_lambda_name = f"{deploy_prefix}process_payments"
process_purchases_lambda_name = f"{deploy_prefix}process_purchases"
process_sales_lambda_name = f"{deploy_prefix}process_sales"
upload_lambda_name = f"{deploy_prefix}upload"

ingest_role = f"{deploy_prefix}ingest-role"
process_payments_role = f"{deploy_prefix}process-payments-role"
process_purchases_role = f"{deploy_prefix}process-purchases-role"
process_sales_role = f"{deploy_prefix}process-sales-role"
warehouse_uploader_role = f"{deploy_prefix}warehouse-uploader-role"

processed_bucket_name = f'{deploy_prefix}processed-bucket'
ingest_bucket_name = f'{deploy_prefix}ingest-bucket'
code_bucket_name = f'{deploy_prefix}code-bucket'


def deploy_lambdas():
    permit = Assign_iam()
    print("Creating policies")
    create_policies(permit)
    print("Creating roles and attaching policies")
    create_roles(permit)
    deploy = Deploy_lambdas()
    print("Creating lambda layers")
    try:
        deploy.create_lambda_layer(
            layer_name="pandas-layer", zipfile="pandas.zip", description="Layer for pandas dependency")
        deploy.create_lambda_layer(
            layer_name="pg8000-layer", zipfile="pg8000.zip", description="Layer for pg8000 dependency")
    except:
        print("Failed on lambda layers")
    print("Creating ingest lambda")
    create_lambdas(permit, deploy, ingest_lambda_name,
                   ingest_role, ingest_handler_name)
    print("Creating process payments lambda")
    create_lambdas(permit, deploy, process_payments_lambda_name,
                   process_payments_role, process_payments_handler_name)
    print("Creating process purchases lambda")
    create_lambdas(permit, deploy, process_purchases_lambda_name,
                   process_purchases_role, process_purchases_handler_name)
    print("Creating process sales lambda")
    create_lambdas(permit, deploy, process_sales_lambda_name,
                   process_sales_role, process_sales_handler_name)
    print("Creating warehouse uploader lambda")
    create_lambdas(permit, deploy, upload_lambda_name,
                   warehouse_uploader_role, upload_handler_name)
    create = Create_resources()

    print("Assigning triggers to ingest bucket")
    print(process_payments_lambda_name,
          process_payments_lambda_name in deploy.lambda_arns)
    if process_payments_lambda_name in deploy.lambda_arns:
        create.assign_bucket_update_event_triggers(
            bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_payments_lambda_name], bucket_folders=['TableName/'])
    print(process_purchases_lambda_name,
          process_purchases_lambda_name in deploy.lambda_arns)
    if process_purchases_lambda_name in deploy.lambda_arns:
        create.assign_bucket_update_event_triggers(
            bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_purchases_lambda_name], bucket_folders=['TableName/'])
    print(process_sales_lambda_name,
          process_sales_lambda_name in deploy.lambda_arns)
    if process_sales_lambda_name in deploy.lambda_arns:
        create.assign_bucket_update_event_triggers(
            bucket_name=ingest_bucket_name, lambda_arn=deploy.lambda_arns[process_sales_lambda_name], bucket_folders=['TableName/'])
    print(upload_lambda_name, upload_lambda_name in deploy.lambda_arns)
    if upload_lambda_name in deploy.lambda_arns:
        create.assign_bucket_update_event_triggers(
            bucket_name=processed_bucket_name, lambda_arn=deploy.lambda_arns[upload_lambda_name], bucket_folders=[''])
    print("Creating scheduled trigger")
    event = Create_events()
    event.create_schedule_event(f'schedule-event-{ingest_lambda_name}', '5')
    lambda_arn = deploy.lambda_arns[ingest_lambda_name]
    response = event.assign_event_target(
        schedule_name=f'schedule-event-{ingest_lambda_name}', target_arn=lambda_arn)
    print("Assigning ingest period result : ", response)


def create_lambdas(permit: Assign_iam, deploy: Deploy_lambdas, lambda_name: str, role_name: str, handler_method: str):
    if handler_method == "":
        print("No handler found")
        return
    print("Create lambda using :", lambda_name, role_name, handler_method)
    deploy.create_lambda(lambda_name=lambda_name, code_bucket=code_bucket_name,
                         role_arn=permit.role_arns[role_name], zip_file=f'{lambda_name}.zip', handler_name=handler_method)


def create_roles(permit: Assign_iam):
    permit.create_lambda_role(role_name=ingest_role)
    permit.attach_custom_policy(
        role_name=ingest_role, policy=f"s3-read-write-{ingest_bucket_name}-{ingest_lambda_name}")
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
    permit.attach_custom_policy(role_name=warehouse_uploader_role,
                                policy=f"s3-read-{processed_bucket_name}-{upload_lambda_name}")
    permit.attach_custom_policy(
        role_name=warehouse_uploader_role, policy=f"cloudwatch-policy-{upload_lambda_name}")
    permit.attach_execution_role(role_name=warehouse_uploader_role)


def create_policies(permit: Assign_iam):
    # processed_bucket_name = 'processed-bucket'
    # ingest_bucket_name = 'ingest-bucket'
    print("Creating ingest policies")
    permit.create_cloudwatch_logging_policy(lambda_name=ingest_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=ingest_lambda_name, read=True, write=True)

    print("Creating payments policies")
    permit.create_cloudwatch_logging_policy(
        lambda_name=process_payments_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_payments_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_payments_lambda_name, read=True, write=True)

    print("Creating purchases policies")
    permit.create_cloudwatch_logging_policy(
        lambda_name=process_purchases_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_purchases_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_purchases_lambda_name, read=True, write=True)

    print("Creating sales policies")
    permit.create_cloudwatch_logging_policy(
        lambda_name=process_sales_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=ingest_bucket_name, lambda_name=process_sales_lambda_name, read=True)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=process_sales_lambda_name, read=True, write=True)

    print("Creating warehouse policies")
    permit.create_cloudwatch_logging_policy(lambda_name=upload_lambda_name)
    permit.create_s3_read_write_policy(
        bucket=processed_bucket_name, lambda_name=upload_lambda_name, read=True)


if __name__ == '__main__':
    deploy_lambdas()
