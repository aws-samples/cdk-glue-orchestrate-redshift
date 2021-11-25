"""
   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 
   Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
   with the License. A copy of the License is located at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
   OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
   and limitations under the License.
 """
# This is the CDK stack to create a Glue workflow performing Redshift benchmark tasks
# Author : Liulu He
# Version: 1.0


from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    core
)


class RedshiftBenchmarkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str
        ,dbname:str
        ,password:str
        ,username:str
        ,host:str
        ,port:int
        #,cluster_id:str
        ,s3_bucket:str
        #,tpcds_root_path:str # root path for tpcds for example s3://redshift-downloads/TPC-DS/2.13/3TB/
        ,rs_role_arn:str
        #,num_runs:int
        #,num_files:int
        #,parallel_level:int
        ,**kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        tpcds_data_path = core.CfnParameter(self, "tpcdsDataPath", type="String",default="s3://redshift-downloads/TPC-DS/2.13/3TB/",description="S3 path root of TPC-DS dataset")
        parallel_level = core.CfnParameter(self, "parallel", type="Number",default=10,description="Number of concurrent queries submitted at the same time")
        num_runs = core.CfnParameter(self, "numRuns", type="Number",default=2,description="Total runs of the test")
        num_files = core.CfnParameter(self, "numFiles", type="Number",default=99,description="Total number of files under tpcds_queries directory")
        # create role for Glue jobs, in prod this could be a bring in value or create by initiation
        policy_statement = iam.PolicyStatement(
                actions=["logs:*","s3:*","ec2:*","iam:*","cloudwatch:*","dynamodb:*","glue:*","redshift:*","redshift-data:*"]
            )

        policy_statement.add_all_resources()

        self.glue_job_role = iam.Role(
            self,
            "Glue-Job-Role-Demo",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        self.glue_job_role.add_to_policy(policy_statement)

        self.dbname=dbname
        self.username=username
        #self.cluster_id=cluster_id
        self.s3_bucket=s3_bucket
        self.password=password
        self.host=host
        self.port=port
        self.num_runs=num_runs.value_as_number
        self.parallel_level=parallel_level.value_as_number
        self.num_files=num_files.value_as_number
        
        ############################### Create Glue jobs#############################################
        ddl_task = self.rs_sql_task("tpcds-benchmark-create-tables", "01-create-tables.sql")
        load_task = self.rs_sql_task("tpcds-benchmark-load-tables","02-load-tables.sql"
            ,parameters={'tpcds_root_path':tpcds_data_path.value_as_string,'role_arn':rs_role_arn})
        

        sequential_query_task = self.run_benchmark_query("tpcds-benchmark-sequential-report","sequential")
        concurrent_query_task = self.run_benchmark_query("tpcds-benchmark-concurrent-report","concurrent")

        ################################## Create workflow #########################################
        redshift_benchmark=glue.CfnWorkflow(self, "redshift-benchmark-pipeline"
            , description="Use TPCDS benchmark Redshift"
            , name="redshift-benchmark"
            , tags={"project":"redshift-benchmark"})
        
        ############### Define pipeline dag by creating trigger and add to workflow#################
        start=glue.CfnTrigger(self, "start-trigger"
            , actions=[glue.CfnTrigger.ActionProperty(job_name=ddl_task.name)] 
            , type="ON_DEMAND" # should be scheduled 
            , description="Start the benchmark process"
            , name="start-redshift-benchmark"
            , tags={"project":"redshift-benchmark"}
            #, schedule=cron(15 12 * * ? *)
            , workflow_name=redshift_benchmark.name)

        load_data=glue.CfnTrigger(self, "load-trigger"
            , actions=[glue.CfnTrigger.ActionProperty(job_name=load_task.name)]
            , type="CONDITIONAL"
            , description="Copy data to Redshift table from S3"
            , name="load-trigger"
            , start_on_creation=True
            , predicate= 
            {
                "conditions": [
                {
                    "logicalOperator": "EQUALS",
                    "jobName": ddl_task.name,
                    "state": "SUCCEEDED",
                }
                ],
                "logical": "ANY",
            }
            , tags={"project":"redshift-benchmark"}
            , workflow_name=redshift_benchmark.name)
        sequential = glue.CfnTrigger(self, "sequential-trigger"
            , actions=[glue.CfnTrigger.ActionProperty(job_name=sequential_query_task.name)]
            , type="CONDITIONAL"
            , description="Perform sequantial run of queries"
            , name="sequential-trigger"
            , start_on_creation=True
            , predicate= 
            {
                "conditions": [
                {
                    "logicalOperator": "EQUALS",
                    "jobName": load_task.name,
                    "state": "SUCCEEDED",
                }
                ],
                "logical": "ANY",
            }
            , tags={"project":"redshift-benchmark"}
            , workflow_name=redshift_benchmark.name)


        concurrent=glue.CfnTrigger(self, "concurrent-trigger"
            , actions=[glue.CfnTrigger.ActionProperty(job_name=concurrent_query_task.name)]
            , type="CONDITIONAL"
            , description="Perform concurrent run of queries"
            , name="concurrent-trigger"
            , start_on_creation=True
            , predicate= 
            {
                "conditions": [
                {
                    "logicalOperator": "EQUALS",
                    "jobName": sequential_query_task.name,
                    "state": "SUCCEEDED",
                }
                ],
                "logical": "ANY",
            }
            , tags={"project":"redshift-benchmark"}
            , workflow_name=redshift_benchmark.name)

        # Add dependency check, resources are created symontinously
        start.add_depends_on(ddl_task)
        load_data.add_depends_on(load_task)
        load_data.add_depends_on(ddl_task)
        sequential.add_depends_on(load_task)
        sequential.add_depends_on(sequential_query_task)
        concurrent.add_depends_on(sequential_query_task)
        concurrent.add_depends_on(concurrent_query_task)
        

      

    def run_benchmark_query(self,job_name,mode):
        return glue.CfnJob(
            self,
            job_name,
            role=self.glue_job_role.role_arn,
            max_capacity=0.0625,
            name = job_name,
            default_arguments = {
                 '--dbname':   self.dbname,
                 '--username': self.username,
                 '--password': self.password,
                 '--host': self.host,
                 '--port': self.port,
                 '--mode':mode,
                 '--sql_script_bucket': self.s3_bucket,
                 '--sql_script_key':'redshift_script/tpcds_queries/',
                 '--num_runs':self.num_runs,
                 '--num_files':self.num_files,
                 '--parallel_level':self.parallel_level
                 #'--extra-py-files':'s3://'+self.s3_bucket+'/glue_script/DBUtils-2.0.2-py3-none-any.whl,s3://'+self.s3_bucket+'/glue_script/psycopg2-2.9.1-cp36-cp36m-linux_x86_64.whl '
                 },
            tags={"project":"redshift-benchmark"},
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location="s3://"+self.s3_bucket+"/glue_script/run_redshift_benchmark.py",
                python_version = "3"
            )
        )


    def rs_sql_task(self,job_name, sql_file,parameters=None):
        if parameters ==None:
            return glue.CfnJob(
                self,
                job_name,
                role=self.glue_job_role.role_arn,
                max_capacity=0.0625,
                name = job_name,
                default_arguments = {
                 '--dbname':   self.dbname,
                 '--username': self.username,
                 '--password': self.password,
                 '--host': self.host,
                 '--port': self.port,
                 '--sql_script_bucket': self.s3_bucket,
                 '--sql_script_key':'redshift_script/'+sql_file, # Only difference for Redshift SQL task
                 '--tpcds_root_path':'NA',
                 '--role_arn':'NA'
                 },
                tags={"project":"redshift-benchmark"},
                command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location="s3://"+self.s3_bucket+"/glue_script/submit_redshift_sql.py",
                python_version = "3"
                )
            )
        else:
            return glue.CfnJob(
                self,
                job_name,
                role=self.glue_job_role.role_arn,
                max_capacity=0.0625,
                name = job_name,
                default_arguments = {
                 '--dbname':   self.dbname,
                 '--username': self.username,
                 '--password': self.password,
                 '--host': self.host,
                 '--port': self.port,
                 '--sql_script_bucket': self.s3_bucket,
                 '--sql_script_key':'redshift_script/'+sql_file, # Only difference for Redshift SQL task
                 '--tpcds_root_path':parameters['tpcds_root_path'],
                 '--role_arn':parameters['role_arn']
                 },
                tags={"project":"redshift-benchmark"},
                command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                script_location="s3://"+self.s3_bucket+"/glue_script/submit_redshift_sql.py",
                python_version = "3"
                )
            )

