"""
   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 
   Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
   with the License. A copy of the License is located at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
   OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
   and limitations under the License.
 """

from aws_cdk import aws_redshift as redshift
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import core



class RedshiftStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct, id: str,
        vpc,
        #ec2_instance_type: str,
        #master_user: str,
        #password:str,
        #node_num:int,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)
        rs_instance_type = core.CfnParameter(self, "rsInstanceType", type="String",default="ra3.xlplus",allowed_values =["dc2.large","dc2.8xlarge","ra3.xlplus","ra3.4xlarge","ra3.16xlarge"],description="Redshift instance type")
        rs_node_count = core.CfnParameter(self, "rsSize", type="Number",default=2,description="Number of Redshift nodes")
        rs_username = core.CfnParameter(self, "username", type="String",default="admin",description="Redshift super username")
        rs_password = core.CfnParameter(self, "password", type="String",default="Admin1234",no_echo=True,description="Redshift superuser password")

        self._rs_cluster_role = iam.Role(
            self, "redshiftClusterRole",
            assumed_by=iam.ServicePrincipal(
                "redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                )
            ]
        )


        # Subnet Group for Cluster
        demo_cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "redshiftDemoClusterSubnetGroup",
            subnet_ids=vpc.get_vpc_public_subnet_ids,
            description="Redshift Demo Cluster Subnet Group"
        )

        # create redshift security group
        self.redshift_sg = ec2.SecurityGroup( self,
            id="redshiftSecurityGroup",
            vpc=vpc.get_vpc,
            security_group_name=f"redshift_sg",
            description="Security Group for Redshift")
        self.redshift_sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(5439), 'Public login access')


        if rs_node_count.value==1:
            cluster_type='single-node'
            self.demo_cluster = redshift.CfnCluster(self,
                "redshiftDemoCluster",
                cluster_type=cluster_type,
                cluster_identifier = "benchmark",
                db_name="dev",
                master_username = rs_username.value_as_string,
                port=5439,
                publicly_accessible=True,
                master_user_password=rs_password.value_as_string,
                iam_roles=[self._rs_cluster_role.role_arn],
                node_type=f"{rs_instance_type.value_as_string}",
                cluster_subnet_group_name=demo_cluster_subnet_group.ref,
                vpc_security_group_ids=[self.redshift_sg.security_group_id]
            )
        else:
            cluster_type='multi-node'
            self.demo_cluster = redshift.CfnCluster(self,
                "redshiftDemoCluster",
                cluster_type=cluster_type,
                cluster_identifier = "benchmark",
                db_name="dev",
                master_username = rs_username.value_as_string,
                port=5439,
                publicly_accessible=True,
                master_user_password=rs_password.value_as_string,
                iam_roles=[self._rs_cluster_role.role_arn],
                node_type=f"{rs_instance_type.value_as_string}",
                cluster_subnet_group_name=demo_cluster_subnet_group.ref,
                number_of_nodes = rs_node_count.value_as_number,
                vpc_security_group_ids=[self.redshift_sg.security_group_id]
            )

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_1 = core.CfnOutput(
            self,
            "RedshiftCluster",
            value=f"{self.demo_cluster.attr_endpoint_address}",
            description=f"RedshiftCluster Endpoint"
        )
        output_2 = core.CfnOutput(
            self,
            "RedshiftClusterId",
            value=(
                f"{self.demo_cluster.cluster_identifier}"
        ),
            description=f"Redshift Cluster id"
        )
        output_3 = core.CfnOutput(
            self,
            "RedshiftIAMRole",
            value=(
                f"{self._rs_cluster_role.role_arn}"
            ),
            description=f"Redshift Cluster IAM Role Arn"
        )

    @property
    def get_security_group(self):
        return self.redshift_sg
    @property
    def get_cluster(self):
        return self.demo_cluster
    @property
    def get_role_arn(self):
        return self._rs_cluster_role.role_arn
    
    