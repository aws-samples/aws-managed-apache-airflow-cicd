from aws_cdk import (
    RemovalPolicy, CfnOutput,
    aws_s3 as s3,
    aws_ssm as ssm,
)
from constructs import Construct


class Bucket(Construct):

    def __init__(self, scope: Construct, id_: str) -> None:
        super().__init__(scope, id_)

        # S3 Bucket
        bucket = s3.Bucket(
            self,
            "WhatsNewBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        ssm.StringParameter(
            self,
            "BucketArn",
            parameter_name="airflow-bucket-arn",
            string_value=bucket.bucket_arn
        )

        CfnOutput(
            self,
            "BucketName",
            value=bucket.bucket_name
        )
