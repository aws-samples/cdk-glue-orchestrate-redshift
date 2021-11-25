#!/usr/bin/env python3
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
    aws_ec2 as ec2,
    core,
)
from redshift_benchmark.lib.cdkVPCStack import VPCStack
from redshift_benchmark.lib.cdkRedshiftStack import RedshiftStack
from redshift_benchmark.lib.cdkInitialAssets import S3Assets
from redshift_benchmark.redshiftBenchmarkStack import RedshiftBenchmarkStack



app = core.App()
    

#################### Upload scripts to S3 that could be inferred by following tasks ######################
asset = S3Assets(app, "repository",local_directory="scripts")

############ Set up VPC and redshift cluster, redshift cluster will reside in public subnet ##############
vpc_stack = VPCStack(app,"vpc-stack")
redshift_stack = RedshiftStack(app,"redshift-stack",vpc_stack)

# Use glue workflow and jobs to conduct benchmark tasks include parallel query execution and concurrent query execution
benchmark_workflow = RedshiftBenchmarkStack(app,"benchmark-workflow"
    ,dbname=redshift_stack.get_cluster.db_name
    ,host=redshift_stack.get_cluster.attr_endpoint_address
    ,port=redshift_stack.get_cluster.attr_endpoint_port
    ,username=redshift_stack.get_cluster.master_username
    ,password=redshift_stack.get_cluster.master_user_password
    ,s3_bucket=asset.get_bucket
    ,rs_role_arn=redshift_stack.get_role_arn
)
   
app.synth()
