# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from airflow import settings, AirflowException
from azure.mgmt.hdinsight.models import ClusterCreateProperties, ClusterDefinition, ComputeProfile, Role, \
    LinuxOperatingSystemProfile, OsProfile, \
    StorageProfile, StorageAccount, OSType, Tier, HardwareProfile

from airflowhdi.hooks import AzureHDInsightHook
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults


class AzureHDInsightCreateClusterOperator(BaseOperator):
    """
    See https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

    :param azure_conn_id: connection id of a service principal
            which will be used to delete Hdinsight cluster
    :type azure_conn_id: str
    :param cluster_name: cluster name of will  creating
    :type cluster_name: str
    """

    template_fields = ['cluster_params']

    #to allow deep nested templatization by airflow on the entire cluster param spec
    ClusterCreateProperties.template_fields = ['cluster_definition', 'compute_profile', 'storage_profile']
    ClusterDefinition.template_fields = ['configurations']
    ComputeProfile.template_fields = ['roles']
    Role.template_fields = ['os_profile']
    OsProfile.template_fields = ['linux_operating_system_profile']
    LinuxOperatingSystemProfile.template_fields = ['username', 'password']
    StorageProfile.template_fields = ['storageaccounts']
    StorageAccount.template_fields = ['name', 'key', 'container']

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 cluster_params: ClusterCreateProperties,
                 azure_conn_id='azure_hdinsight_default',
                 *args,
                 **kwargs
                 ):
        super(AzureHDInsightCreateClusterOperator, self).__init__(*args, **kwargs)

        self.cluster_name = cluster_name
        self.cluster_params = cluster_params
        self.azure_conn_id = azure_conn_id

    def execute(self, context):
        azure_hook = AzureHDInsightHook(azure_conn_id=self.azure_conn_id)
        self.log.info("Executing HDInsightCreateClusterOperator ")
        azure_hook.create_cluster(self.cluster_params, self.cluster_name)
        self.log.info("Finished executing HDInsightCreateClusterOperator")


class ConnectedAzureHDInsightCreateClusterOperator(AzureHDInsightCreateClusterOperator):
    """
    An extension of the AzureHDInsightCreateClusterOperator which allows
    getting credentials and other common properties for ClusterCreateProperties
    from a connection
    """
    # make sure these are imported. eval() below needs them.
    param_field_types = [OSType, Tier, ClusterDefinition, ComputeProfile, Role, \
                         HardwareProfile, LinuxOperatingSystemProfile, OsProfile, \
                         StorageProfile, StorageAccount]

    @apply_defaults
    def __init__(self,
                 azure_conn_id=None,
                 hdi_conn_id=None,
                 *args,
                 **kwargs
                 ):
        session = settings.Session()
        azure_conn = session.query(Connection).filter(Connection.conn_id == azure_conn_id).first()
        hdi_conn = session.query(Connection).filter(Connection.conn_id == hdi_conn_id).first()
        cluster_params = eval(compile(hdi_conn.extra, 'file', 'eval'))

        if azure_conn:
            super(ConnectedAzureHDInsightCreateClusterOperator, self).__init__(*args, **dict(
                kwargs, params=azure_conn.extra_dejson, azure_conn_id=azure_conn_id,
                cluster_params=cluster_params))
        else:
            raise AirflowException(
                f"Connection with conn_id {azure_conn_id} not found"
            )

    def execute(self, context):
        return super(ConnectedAzureHDInsightCreateClusterOperator, self).execute(
            context=context)