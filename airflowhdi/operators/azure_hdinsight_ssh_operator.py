from airflow import AirflowException
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.decorators import apply_defaults

from airflowhdi.hooks import AzureHDInsightHook


class AzureHDInsightSshOperator(SSHOperator):
    """
    Uses the AzureHDInsightHook and SSHHook to run an SSH command on the master node of the given HDInsight cluster
    """
    @apply_defaults
    def __init__(self,
                 cluster_name,
                 azure_conn_id='azure_hdinsight_default',
                 *args,
                 **kwargs
                 ):
        """
        The SSH username and password are fetched from the azure connection passed.
        The SSH endpoint and port of the cluster is also fetched
        using the :meth:`airflowhdi.hooks.AzureHDInsightHook.get_cluster_state` method.

        :param cluster_name: Unique cluster name of the HDInsight cluster
        :type cluster_name: str
        :param azure_conn_id: connection ID of the Azure HDInsight cluster.
        :param command: command to execute on remote host. (templated)
        :type command: str
        :param timeout: timeout (in seconds) for executing the command.
        :type timeout: int
        :param environment: a dict of shell environment variables. Note that the
            server will reject them silently if `AcceptEnv` is not set in SSH config.
        :type environment: dict
        :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
        :type do_xcom_push: bool
        :param get_pty: request a pseudo-terminal from the server. Set to ``True``
            to have the remote process killed upon task timeout.
            The default is ``False`` but note that `get_pty` is forced to ``True``
            when the `command` starts with ``sudo``.
        :type get_pty: bool
        """
        super(AzureHDInsightSshOperator, self).__init__(*args, **kwargs)

        self.cluster_name = cluster_name
        self.azure_conn_id = azure_conn_id

    def execute(self, context):
        """
        :raises AirflowException: when the SSH endpoint of the HDI cluster cannot be found
        """
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