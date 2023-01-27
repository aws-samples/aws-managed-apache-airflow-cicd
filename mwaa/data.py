from aws_cdk import (
    RemovalPolicy, CfnOutput,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
)
from constructs import Construct


class Data(Construct):

    def __init__(self, scope: Construct, id_: str, environment_name: str) -> None:
        super().__init__(scope, id_)

        self.topic = sns.Topic(
            self,
            "EmailTopic",
            topic_name=f"{environment_name}-topic",
            display_name=f"{environment_name}-topic"
        )

        CfnOutput(
            self,
            "TopicArn",
            value=self.topic.topic_arn
        )

        self.table = dynamodb.Table(
            self,
            "TableWhatsNew",
            table_name=f"{environment_name}-whatsnew",
            partition_key=dynamodb.Attribute(
                name="post_month",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="post_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
