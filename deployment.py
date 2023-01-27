import os

from aws_cdk import Stage, Stack, Environment
from constructs import Construct

from network.vpc import VPC
from mwaa.bucket import Bucket
from mwaa.mwaa import MWAA
from mwaa.data import Data
from cicd.pipeline import MWAADagsPipeline


class AirflowCICD(Stage):
    def __init__(self, scope: Construct, id_: str, environment_name: str, tags: dict, **kwargs) -> None:
        super().__init__(scope, id_, **kwargs)

        #####################################################################
        # Bucket
        #####################################################################
        airflow_bucket = Stack(
            self,
            "Bucket",
            env=Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
            tags=tags,
        )

        Bucket(
            airflow_bucket,
            "S3Bucket"
        )

        #####################################################################
        # Network
        #####################################################################
        network = Stack(
            self,
            "Network",
            env=Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
            tags=tags,
        )

        VPC(
            network,
            "VPC",
            cidr="10.10.0.0/16",
            environment_name=environment_name
        )

        #####################################################################
        # MWAA
        #####################################################################
        mwaa_infra = Stack(
            self,
            "MWAA",
            env=Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
            tags=tags,
        )

        data = Data(
            mwaa_infra,
            "Data",
            environment_name=environment_name,
        )

        MWAA(
            mwaa_infra,
            "Airflow",
            environment_name=environment_name,
            topic_arn=data.topic.topic_arn,
            table_arn=data.table.table_arn,
            tags=tags,
        )

        #####################################################################
        # CI/CD
        #####################################################################
        cicd = Stack(
            self,
            "CICD",
            env=Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]),
            tags=tags,
        )

        MWAADagsPipeline(
            cicd,
            "Pipeline",
            branch_name="master"
        )




