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
import os

from azure.common.client_factory import get_client_from_json_dict
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.mgmt.hdinsight.models import ClusterCreateProperties, ClusterCreateParametersExtended, ClusterGetProperties, \
    ErrorResponseException
from msrestazure.azure_operation import AzureOperationPoller

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class AzureHDInsightHook(BaseHook):

    def __init__(self, azure_conn_id='azure_default'):
        super(AzureHDInsightHook, self).__init__(azure_conn_id)

        self.conn_id = azure_conn_id
        connection = self.get_connection(azure_conn_id)
        extra_options = connection.extra_dejson

        self.client = self.get_conn()
        self.resource_group_name = str(extra_options.get("resource_group_name"))
        self.resource_group_location = str(extra_options.get("resource_group_location"))

    def get_conn(self):
        """
        Return a HDInsight client.

        This hook requires a service principal in order to work.
        You can create a service principal from the az CLI like so:
        az ad sp create-for-rbac --name localtest-sp-rbac --skip-assignment --sdk-auth > local-sp.json

        References
        https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
        https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python#authentication-example-using-a-service-principal
        https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?view=azure-python&tabs=cmd#authenticate-with-a-json-dictionary

        :return: HDInsight management client
        :rtype: HDInsightManagementClient
        """
        conn = self.get_connection(self.conn_id)
        servicePrincipal = conn.extra_dejson.get('servicePrincipal', False)
        if servicePrincipal:
            return get_client_from_json_dict(HDInsightManagementClient, servicePrincipal)

        credentials = ServicePrincipalCredentials(
            client_id=servicePrincipal['clientId'],
            secret=servicePrincipal['clientSecret'],
            tenant=servicePrincipal['tenantId']
        )

        subscription_id = servicePrincipal['subscriptionId']
        return HDInsightManagementClient(credentials, str(subscription_id))

    def create_cluster(self, cluster_create_properties: ClusterCreateProperties, cluster_name):
        """
        References
        https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

        :return:
        """
        cluster_deployment: AzureOperationPoller = self.client.clusters.create(
            cluster_name=cluster_name,
            resource_group_name=self.resource_group_name,
            parameters=ClusterCreateParametersExtended(
                location=self.resource_group_location,
                tags={},
                properties=cluster_create_properties
            ))
        # cluster_deployment.wait() - this blocks
        self.log.info("Cluster deployment started %s: %s", cluster_name, cluster_deployment.status())

    def delete_cluster(self, cluster_name):
        """
        References
        https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

        :return:
        """
        delete_poller: AzureOperationPoller = self.client.clusters.delete(self.resource_group_name,
                                                                          cluster_name=cluster_name)
        delete_poller.wait()
        self.log.info(delete_poller.result())
        return delete_poller.result()

    def get_cluster_state(self, cluster_name):
        try:
            state: ClusterGetProperties = self.client.clusters.get(self.resource_group_name,
                                                                   cluster_name).properties
        except ErrorResponseException as e:
            if e.response.status_code == 404:
                self.log.info("Cluster not found: %s", cluster_name)
                return None
            else:
                raise AirflowException(e)
        return state
