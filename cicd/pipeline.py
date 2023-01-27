from aws_cdk import (
    CfnOutput, RemovalPolicy,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_s3 as s3,
    aws_ssm as ssm,
)
from constructs import Construct


class MWAADagsPipeline(Construct):

    def __init__(self, scope: Construct, construct_id: str, branch_name: str) -> None:
        super().__init__(scope, construct_id)

        bucket_arn = ssm.StringParameter.value_for_typed_string_parameter_v2(
            self,
            parameter_name="airflow-bucket-arn",
            type=ssm.ParameterValueType.STRING
        )

        target_bucket = s3.Bucket.from_bucket_attributes(
            self,
            "Bucket",
            bucket_arn=bucket_arn
        )

        src_name = "airflow_dags"

        # CodeCommit Repository
        src_repo = codecommit.Repository(
            self,
            "SourceRepo",
            repository_name=src_name
        )

        CfnOutput(
            self,
            "CloneHTTPURL",
            value=src_repo.repository_clone_url_http
        )

        # CodeBuild
        codecopy_build = codebuild.Project(
            self,
            "UnitTestBuild",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_4
            ),
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "python": "3.9"
                            }
                        },
                        "build": {
                            "commands": [
                                "python --version",
                            ]
                        }
                    },
                    "artifacts": {
                        "base-directory": "dags",
                        "files": ["**/*"]
                    }
                }
            )
        )

        source_code_output = codepipeline.Artifact("SourceCodeOutput")
        unittest_build_output = codepipeline.Artifact("UnitTestBuildOutput")

        artifact_bucket = s3.Bucket(
            self,
            "PipelineBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        codepipeline.Pipeline(
            self,
            "AirflowDagsPipeline",
            pipeline_name="airflow-dags-pipeline",
            artifact_bucket=artifact_bucket,
            stages=[
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[
                        codepipeline_actions.CodeCommitSourceAction(
                            action_name="Source",
                            repository=src_repo,
                            output=source_code_output,
                            branch=branch_name
                        )
                    ]
                ),
                codepipeline.StageProps(
                    stage_name="Build",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="Build",
                            project=codecopy_build,
                            input=source_code_output,
                            outputs=[unittest_build_output]
                        )
                    ]
                ),
                codepipeline.StageProps(
                    stage_name="Deploy",
                    actions=[
                        codepipeline_actions.S3DeployAction(
                            action_name="S3Deploy",
                            bucket=target_bucket,
                            extract=True,
                            object_key="dags",
                            input=unittest_build_output
                        )
                    ]
                )
            ]
        )
