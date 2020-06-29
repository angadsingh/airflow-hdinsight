from airflowhdi.hooks import AzureHDInsightHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AzureHDInsightDeleteClusterOperator(BaseOperator):
    """
    .. seealso::
            See the documentation of :class:`airflowhdi.hooks.AzureHDInsightHook`
            for explanation on the parameters of this operator
    """
    @apply_defaults
    def __init__(self,
                 cluster_name,
                 azure_conn_id='azure_hdinsight_default',
                 *args,
                 **kwargs
                 ):
        super(AzureHDInsightDeleteClusterOperator, self).__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.azure_conn_id = azure_conn_id

    def execute(self, context):
        azure_hook = AzureHDInsightHook(azure_conn_id=self.azure_conn_id)
        self.log.info("Executing HDInsightDeleteClusterOperator ")
        azure_hook.delete_cluster(cluster_name=self.cluster_name)
        self.log.info("Finished executing HDInsightDeleteClusterOperator")
