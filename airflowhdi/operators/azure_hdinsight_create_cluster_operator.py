from airflow import settings, AirflowException
from azure.mgmt.hdinsight.models import ClusterCreateProperties, ClusterDefinition, ComputeProfile, Role, \
    LinuxOperatingSystemProfile, OsProfile, \
    StorageProfile, StorageAccount, OSType, Tier, HardwareProfile

from airflowhdi.hooks import AzureHDInsightHook
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults


class AzureHDInsightCreateClusterOperator(BaseOperator):
    """
    .. seealso::
            See the documentation of :class:`airflowhdi.hooks.AzureHDInsightHook`
            for explanation on the parameters of this operator
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
        """
        :param azure_conn_id: connection ID of the Azure HDInsight cluster.
        :type azure_conn_id: string
        :param cluster_name: Unique cluster name of the HDInsight cluster
        :type cluster_name: str
        :param cluster_params: the :class:`azure.mgmt.hdinsight.models.ClusterCreateProperties` representing the HDI cluster spec.
            You can explore some sample specs `here <https://github.com/Azure-Samples/hdinsight-python-sdk-samples>`_.
            This python object follows the same structure as the `HDInsight arm template <https://docs.microsoft.com/en-us/azure/templates/microsoft.hdinsight/2018-06-01-preview/clusters>`_.

            :download:`Example ClusterCreateProperties<../../examples/azure_hdi_cluster_conn.py>`
        :type cluster_params: ClusterCreateProperties
        """
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
    An extension of the :class:`AzureHDInsightCreateClusterOperator` which allows
    getting credentials and other common properties for :class:`azure.mgmt.hdinsight.models.ClusterCreateProperties`
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
        """
        :param azure_conn_id: connection ID of the Azure HDInsight cluster.
        :type azure_conn_id: string
        :param hdi_conn_id: connection ID of the connection that contains
            a :class:`azure.mgmt.hdinsight.models.ClusterCreateProperties` object in its extra field
        :type hdi_conn_id: str
        :param cluster_params: cluster creation spec
        :type cluster_params: ClusterCreateProperties
        :param cluster_name: Unique cluster name of the HDInsight cluster
        :type cluster_name: str
        """
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