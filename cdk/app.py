import os

from aws_cdk import (
    App, Duration, Size, Stack,  
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda
)

from constructs import Construct

class Application(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.build_lambda_func()


    def build_lambda_func(self):
        # Define role to determine scope of permissions function can assume
        lambda_role = iam.Role(self,"Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="rubix-importer-blisspoint-lambda-role",
            description=""
        )
        
        # Attach policies to the role 
        lambda_role.attach_inline_policy(iam.Policy(self,"secrets-policy",
            statements=[iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=["*"] 
                )]
            )
        )
        
        lambda_role.attach_inline_policy(iam.Policy(self,"s3-policy",
            statements=[iam.PolicyStatement(
                actions=[
                    "s3:*",
                    "s3-object-lambda:*"
                ],
                resources=["*"]
                )]
            )
        )

        lambda_role.attach_inline_policy(iam.Policy(self,"role-policy",
            statements=[iam.PolicyStatement(
                actions=[
                    "iam:PassRole",
                    "iam:ListRolePolicies",
                    "iam:ListAttachedRolePolicies",
                    "iam:GetRole",
                    "iam:GetRolePolicy",
                    "iam:SimulatePrincipalPolicy",
                    "iam:GetPolicy",
                    "iam:GetPolicyVersion",
                    "iam:ListRoles"
                ],
                resources=["*"] 
                )]
            )
        )

        lambda_role.attach_inline_policy(iam.Policy(self,"lambda-policy",
            statements=[iam.PolicyStatement(
                actions=[
                    "lambda:InvokeFunction",
                    "lambda:UpdateFunctionCode",
                    "lambda:GetFunction",
                    "lambda:GetAccountSettings",
                    "lambda:GetEventSourceMapping",
                    "lambda:GetFunctionConfiguration",
                    "lambda:GetFunctionCodeSigningConfig",
                    "lambda:GetFunctionConcurrency",
                    "lambda:ListEventSourceMappings",
                    "lambda:ListFunctions",
                    "lambda:ListTags"
                ],
                resources=["*"] 
                )]
            )
        )

        lambda_role.attach_inline_policy(iam.Policy(self,"resource-policy",
            statements=[iam.PolicyStatement(
                actions=[
                    "cloudformation:DescribeStacks",
                    "cloudformation:ListStackResources",
                    "cloudwatch:ListMetrics",
                    "cloudwatch:GetMetricData",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcs",
                    "kms:ListAliases",
                    "logs:DescribeLogGroups",
                    "states:DescribeStateMachine",
                    "states:ListStateMachines",
                    "tag:GetResources",
                    "xray:GetTraceSummaries",
                    "xray:BatchGetTraces",
                    "logs:*"
                ],
                resources=["*"] 
                )]
            )
        )

        self.prediction_lambda = _lambda.DockerImageFunction(
            scope=self,
            id="BlisspointDownloader",
            function_name="blisspoint_downloader",
            timeout=Duration.seconds(600),
            memory_size=1024, #in MB
            ephemeral_storage_size=Size.mebibytes(1024), #MiB
            role=lambda_role,
            # Use aws_cdk.aws_lambda.DockerImageCode.from_image_asset to build
            # a docker image on deployment
            code=_lambda.DockerImageCode.from_image_asset(
                # Directory relative to where you execute cdk deploy
                # contains a Dockerfile with build instructions
                directory = os.path.relpath("src/lambda_function/", start=os.path.abspath(__file__) )
            ),
        )

        # Run every day at 10AM UTC
        rule = events.Rule(
            self, "Rule",
            schedule=events.Schedule.cron(
                minute='0',
                hour='10',
                month='*',
                week_day='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(self.prediction_lambda))

app = App()
Application(app, "rubix-blisspoint-importer")
app.synth()
