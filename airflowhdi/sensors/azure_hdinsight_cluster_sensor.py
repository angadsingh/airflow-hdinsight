from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflowhdi.hooks import AzureHDInsightHook
from azure.mgmt.hdinsight.models import HDInsightClusterProvisioningState
from airflow.exceptions import AirflowException


class AzureHDInsightClusterSensor(BaseSensorOperator):
    """
    Asks for the state of the HDInsight cluster until it achieves the
    desired state: provisioning or running
    If it fails the sensor errors, failing the task.

    .. seealso::
            See the documentation of :class:`airflowhdi.hooks.AzureHDInsightHook`
            for explanation on the parameters of this operator
    """

    PROV_ONLY_TERMINAL_STATES = [HDInsightClusterProvisioningState.in_progress,
                           HDInsightClusterProvisioningState.deleting]
    NON_TERMINAL_STATES = [HDInsightClusterProvisioningState.in_progress,
                                 HDInsightClusterProvisioningState.deleting,
                                 HDInsightClusterProvisioningState.succeeded]
    FAILED_STATE = [HDInsightClusterProvisioningState.failed,
                    HDInsightClusterProvisioningState.canceled]
    template_fields = ['cluster_name']

    def __init__(
            self,
            cluster_name,
            azure_conn_id='azure_hdinsight_default',
            provisioning_only=False,
            poke_interval=20,
            mode="poke",
            *args,
            **kwargs
    ):
        """
        :param cluster_name: name of the cluster to check the state of
        :type cluster_name: str
        :param azure_conn_id: azure connection to get config from
        :type azure_conn_id: str
        :param provisioning_only: poke up till provisioning only if `True`,
            else poke till the cluster hasn't achieved a terminal state
        :type provisioning_only: bool
        """

        super().__init__(
            poke_interval=poke_interval,
            mode=mode,
            *args,
            **kwargs
        )

        self.provisioning_only = provisioning_only
        self.cluster_name = cluster_name
        self.azure_conn_id = azure_conn_id

    def poke(self, context):
        azure_hook = AzureHDInsightHook(azure_conn_id=self.azure_conn_id)
        state = azure_hook.get_cluster_state(self.cluster_name)

        if state is None:
            # cluster does not exist
            # or got deleted
            return True

        self.log.info("Cluster state for cluster %s is %s/%s",
                      self.cluster_name,
                      state.provisioning_state,
                      state.cluster_state)

        if self.provisioning_only:
            if state.provisioning_state in self.PROV_ONLY_TERMINAL_STATES:
                return False
        else:
            if state.provisioning_state in self.NON_TERMINAL_STATES:
                return False

        if state.provisioning_state in self.FAILED_STATE:
            raise AirflowException(f"Cluster {self.cluster_name} has "
                                   + f"{state.provisioning_state}/{state.cluster_state} with error: {state.errors}")

        return True
