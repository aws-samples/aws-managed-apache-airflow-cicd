import os

from aws_cdk import (
    Aws,
    aws_mwaa as mwaa,
    aws_iam as iam,
    aws_ssm as ssm,
)
from constructs import Construct


class MWAA(Construct):

    def __init__(self, scope: Construct, id_: str, environment_name: str,
                 topic_arn: str, table_arn: str, tags: dict) -> None:
        super().__init__(scope, id_)

        bucket_arn = ssm.StringParameter.value_for_typed_string_parameter_v2(
            self,
            parameter_name="airflow-bucket-arn",
            type=ssm.ParameterValueType.STRING
        )

        # Execution Role
        execution_role = iam.Role(
            self,
            "MWAAExecutionRole",
            role_name="MWAAExecutionRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
            inline_policies={
                "MWAAExecutionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "airflow:PublishMetrics",
                            ],
                            resources=[
                                f"arn:aws:airflow:{Aws.REGION}:{Aws.ACCOUNT_ID}:environment/{environment_name}"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.DENY,
                            actions=[
                                "s3:ListAllMyBuckets",
                            ],
                            resources=[
                                bucket_arn,
                                f"{bucket_arn}/*"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject*",
                                "s3:GetBucket*",
                                "s3:List*"
                            ],
                            resources=[
                                bucket_arn,
                                f"{bucket_arn}/*"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "logs:CreateLogStream",
                                "logs:CreateLogGroup",
                                "logs:PutLogEvents",
                                "logs:GetLogEvents",
                                "logs:GetLogRecord",
                                "logs:GetLogGroupFields",
                                "logs:GetQueryResults"
                            ],
                            resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:airflow-{environment_name}-*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "logs:DescribeLogGroups"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "cloudwatch:PutMetricData"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "sqs:ChangeMessageVisibility",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl",
                                "sqs:ReceiveMessage",
                                "sqs:SendMessage"
                            ],
                            resources=[f"arn:aws:sqs:{Aws.REGION}:*:airflow-celery-*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Decrypt",
                                "kms:DescribeKey",
                                "kms:GenerateDataKey*",
                                "kms:Encrypt"
                            ],
                            not_resources=[
                                f"arn:aws:kms:*:{Aws.ACCOUNT_ID}:key/*"
                            ],
                            conditions={
                                "StringLike": {
                                    "kms:ViaService": [
                                        f"sqs.{Aws.REGION}.amazonaws.com"
                                    ]
                                }
                            }
                        ),
                    ]
                ),
                "ServiceAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:PutObject",
                            ],
                            resources=[
                                f"{bucket_arn}/*"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "dynamodb:BatchWriteItem",
                                "dynamodb:query"
                            ],
                            resources=[
                                table_arn
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "SNS:Publish"
                            ],
                            resources=[
                                topic_arn
                            ]
                        ),
                    ]
                )
            }
        )

        sg_ids = ssm.StringListParameter.value_for_typed_list_parameter(
            self,
            parameter_name="airflow-sg-ids",
            type=ssm.ParameterValueType.AWS_EC2_SECURITYGROUP_ID
        )

        subnet_ids = ssm.StringListParameter.value_for_typed_list_parameter(
            self,
            parameter_name="airflow-private-subnet-ids",
            type=ssm.ParameterValueType.AWS_EC2_SUBNET_ID
        )

        mwaa_config = {
            # kms_key="kmsKey",
            "airflow_version": "2.4.3",
            "execution_role_arn": execution_role.role_arn,
            "dag_s3_path": "dags",
            "environment_class": "mw1.small",
            "max_workers": 10,
            "min_workers": 1,
            "schedulers": 2,
            "source_bucket_arn": bucket_arn,
            "tags": tags,
            "webserver_access_mode": "PUBLIC_ONLY",
            "weekly_maintenance_window_start": "SUN:03:30"
        }

        requirements_s3_object_version = os.environ.get("REQUIREMENTS_S3_OBJ_VER")
        if requirements_s3_object_version is not None:
            mwaa_config["requirements_s3_object_version"] = requirements_s3_object_version
            mwaa_config["requirements_s3_path"] = "requirements/requirements.txt"

        mwaa.CfnEnvironment(
            self,
            "MWAAEnvironment",
            name=environment_name,
            # this part is all optional
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                ),
                scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                ),
                task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                ),
                webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                ),
                worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                )
            ),
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=sg_ids,
                subnet_ids=subnet_ids
            ),
            **mwaa_config
        )


