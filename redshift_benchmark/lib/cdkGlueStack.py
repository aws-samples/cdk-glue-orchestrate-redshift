"""
   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 
   Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
   with the License. A copy of the License is located at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
   OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
   and limitations under the License.
 """
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    core
)

class RSGlueJob (core.Stack):

    def __init__(self, scope: core.Construct, id: str, script_path:str, job_name:str
        ,dbname:str,cluster_id:str,
        #host:str,
        #port:int,
        username:str,
        #password:str,
        sql_script_bucket:str,sql_script_key:str,dependent_packages_path:str
        ,**kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        policy_statement = iam.PolicyStatement(
                actions=["logs:*","s3:*","ec2:*","iam:*","cloudwatch:*","dynamodb:*","glue:*","redshift:*","redshift-data:*"]
            )

        policy_statement.add_all_resources()

        glue_job_role = iam.Role(
            self,
            "Glue-Job-Role-Demo",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        glue_job_role.add_to_policy(policy_statement)

        job = glue.CfnJob(
            self,
            "glue-redshift-job",
            role=glue_job_role.role_arn,
            max_capacity=0.0625,
            #glue_version = "2.0",
            name = job_name,
            default_arguments = {
                 '--dbname':   dbname,
                 #'--host':  host,
                 #'--port':  port,
                 '--username': username,
                 '--cluster_id':cluster_id,
                 #'--password': password,
                 '--sql_script_bucket': sql_script_bucket,
                 '--sql_script_key':sql_script_key,
                 '--extra-py-files':dependent_packages_path},
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location=script_path,
                python_version = "3"
            )
        )

class GlueCheckCondition (core.Stack):

    def __init__(self, scope: core.Construct, id: str,
        job_name:str,
        checker_type:str,
        script_path:str,
        **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        policy_statement = iam.PolicyStatement(
                actions=["logs:*","s3:*","ec2:*","iam:*","cloudwatch:*","dynamodb:*","glue:*","redshift:*","redshift-data:*"]
            )

        policy_statement.add_all_resources()

        glue_job_role = iam.Role(
            self,
            "Glue-Job-Role-Demo",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        glue_job_role.add_to_policy(policy_statement)

        job = glue.CfnJob(
            self,
            "glue-checker-job",
            role=glue_job_role.role_arn,
            max_capacity=0.0625,
            #glue_version = "2.0",
            name = job_name,
            default_arguments = {
                 '--type':   checker_type,
                 },
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location=script_path,
                python_version = "3"
            )
        )
    

