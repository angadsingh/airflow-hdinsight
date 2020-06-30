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
    """
    Uses the HDInsightManagementClient from the `HDInsight SDK for Python <https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python>`_
    to expose several operations on an HDInsight cluster: get cluster state, create, delete.

    :download:`Example HDInsight connection<../../examples/azure_hdi_conn.json>`
    """
    def __init__(self, azure_conn_id='azure_default'):
        """
        :param azure_conn_id: connection ID of the Azure HDInsight cluster. See example above.
        :type azure_conn_id: string
        """
        super(AzureHDInsightHook, self).__init__(azure_conn_id)

        self.conn_id = azure_conn_id
        connection = self.get_connection(azure_conn_id)
        extra_options = connection.extra_dejson

        self.client = self.get_conn()
        self.resource_group_name = str(extra_options.get("resource_group_name"))
        self.resource_group_location = str(extra_options.get("resource_group_location"))

    def get_conn(self) -> HDInsightManagementClient:
        """
        Return a HDInsight Management client from the Azure Python SDK for HDInsight

        This hook requires a service principal in order to work.
        You can create a service principal from the az CLI like so::

          az ad sp create-for-rbac --name localtest-sp-rbac --skip-assignment \\
            --sdk-auth > local-sp.json

        .. seealso::
            * `Create an Azure AD app & service principal in the portal - Microsoft identity platform <https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal>`_
            * `Azure HDInsight SDK for Python <https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python#authentication-example-using-a-service-principal>`_
            * `How to authenticate Python applications with Azure services <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?view=azure-python&tabs=cmd#authenticate-with-a-json-dictionary>`_

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
        Creates an HDInsight cluster

        This operation simply starts the deployment, which happens asynchronously in azure.
        You can call :meth:`~get_cluster_state` for polling on its provisioning.

        .. note::
            This operation is idempotent. If the cluster already exists, this call will simple ignore that fact.
            So this can be used like a "create if not exists" call.

        :param cluster_create_properties: the ClusterCreateProperties representing the HDI cluster spec.
            You can explore some sample specs `here <https://github.com/Azure-Samples/hdinsight-python-sdk-samples>`_.
            This python object follows the same structure as the `HDInsight arm template <https://docs.microsoft.com/en-us/azure/templates/microsoft.hdinsight/2018-06-01-preview/clusters>`_.

            :download:`Example ClusterCreateProperties<../../examples/azure_hdi_cluster_conn.py>`
        :type cluster_create_properties: ClusterCreateProperties
        :param cluster_name: The full cluster name. This is the unique deployment identifier of an
            HDI cluster in Azure, and will be used for fetching its state or submitting jobs to it
            HDI cluster names have the following `restrictions <https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-provision-linux-clusters#cluster-name>`_.
        :type cluster_name: string
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
        Delete and HDInsight cluster

        :param cluster_name: the name of the cluster to delete
        :type cluster_name: string
        """
        delete_poller: AzureOperationPoller = self.client.clusters.delete(self.resource_group_name,
                                                                          cluster_name=cluster_name)
        delete_poller.wait()
        self.log.info(delete_poller.result())
        return delete_poller.result()

    def get_cluster_state(self, cluster_name) -> ClusterGetProperties:
        """
        Gets the cluster state.
        """
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
