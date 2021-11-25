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
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    core
)

import os

class S3Assets (core.Stack):

    def __init__(self, scope: core.Construct, id: str, local_directory:str,**kwargs) -> None:
        super().__init__(scope, id, **kwargs)
    

        self.s3bucket = s3.Bucket(self, 'S3Asset')
        s3deploy.BucketDeployment(self, 's3deploy', sources =[s3deploy.Source.asset(os.getcwd()+ "/"+local_directory)]
            ,destination_bucket=self.s3bucket)


        #core.CfnOutput(self, "S3BucketName", value=self.s3bucket.bucket_name,export_name="s3_script_path")
    @property
    def get_bucket(self):
        return self.s3bucket.bucket_name