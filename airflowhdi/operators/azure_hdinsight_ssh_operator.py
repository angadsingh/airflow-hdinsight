from airflow import AirflowException
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.decorators import apply_defaults

from airflowhdi.hooks import AzureHDInsightHook


class AzureHDInsightSshOperator(SSHOperator):
    @apply_defaults
    def __init__(self,
                 cluster_name,
                 azure_conn_id='azure_hdinsight_default',
                 *args,
                 **kwargs
                 ):
        super(AzureHDInsightSshOperator, self).__init__(*args, **kwargs)

        self.cluster_name = cluster_name
        self.azure_conn_id = azure_conn_id

    def execute(self, context):
        azure_hook = AzureHDInsightHook(azure_conn_id=self.azure_conn_id)
        azure_conn_opts = azure_hook.get_connection(self.azure_conn_id).extra_dejson
        ssh_username = azure_conn_opts['SSH_USER_NAME']
        ssh_password = azure_conn_opts['SSH_PASSWORD']

        state = azure_hook.get_cluster_state(self.cluster_name)
        for endpoint in state.connectivity_endpoints:
            if endpoint.name == 'SSH':
                ssh_endpoint = endpoint.location
                ssh_port = endpoint.port

        if not ssh_endpoint:
            raise AirflowException("Could not find SSH endpoint for cluster {}", self.cluster_name)

        self.ssh_hook = SSHHook(
            remote_host=ssh_endpoint,
            port=ssh_port,
            username=ssh_username,
            password=ssh_password
        )

        self.log.info("Running SSH command on cluster (%s): %s", self.cluster_name, self.command)
        super(AzureHDInsightSshOperator, self).execute(context)