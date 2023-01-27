from aws_cdk import (
    aws_ec2 as ec2,
    aws_ssm as ssm,
)
from constructs import Construct


class VPC(Construct):

    def __init__(self, scope: Construct, id_: str, cidr: str, environment_name: str) -> None:
        super().__init__(scope, id_)

        vpc = ec2.Vpc(
            self,
            "VPC",
            max_azs=2,
            ip_addresses=ec2.IpAddresses.cidr(cidr),
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC,
                    name="Public",
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    name="Private",
                    cidr_mask=24
                )
            ],
            nat_gateways=2,
        )

        sg = ec2.SecurityGroup(
            self,
            "SecurityGroup",
            vpc=vpc,
            description="Security Group for Amazon MWAA Environment mwaa-workshop",
            security_group_name=f"airflow-{environment_name}-sg"
        )

        sg.add_ingress_rule(
            peer=sg,
            connection=ec2.Port.all_traffic()
        )

        ssm.StringListParameter(
            self,
            "SecurityGroupIDs",
            parameter_name="airflow-sg-ids",
            string_list_value=[sg.security_group_id]
        )

        ssm.StringListParameter(
            self,
            "PrivateSubnetIDs",
            parameter_name="airflow-private-subnet-ids",
            string_list_value=[sub.subnet_id for sub in vpc.private_subnets]
        )
