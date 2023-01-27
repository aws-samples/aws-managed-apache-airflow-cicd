#!/usr/bin/env python3
import aws_cdk as cdk

from deployment import AirflowCICD


app = cdk.App()

environment_name = "mwaa-cicd"
AirflowCICD(
    app,
    "AirflowCICD",
    environment_name=environment_name,
    tags={"Owner": "mwaa-cicd"}
)


app.synth()

