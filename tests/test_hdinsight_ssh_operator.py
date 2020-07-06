import unittest

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Connection
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from azure.mgmt.hdinsight.models import ClusterGetProperties, ConnectivityEndpoint
from mock import patch, call

from airflowhdi.operators import AzureHDInsightSshOperator
from tests.test_commons import AZURE_CONN, AZURE_CONN_ID, CLUSTER_NAME, DEFAULT_DAG_ARGS


class TestHDinsightSSHOp(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    def test_execute(self):
        with patch('airflow.settings.Session') as mock_session:
            with patch('airflowhdi.operators.azure_hdinsight_ssh_operator.SSHHook') as ssh_hook:
                with patch('airflowhdi.operators.azure_hdinsight_ssh_operator.AzureHDInsightHook') as mock_hdi_hook:
                    with patch.object(SSHOperator, 'execute') as mock_ssh_op_exec:
                        ssh_endpoint = 'loc'
                        ssh_port = 523

                        mock_session.return_value = UnifiedAlchemyMagicMock(data=[
                            (
                                [call.query(Connection),
                                 call.filter(Connection.conn_id == AZURE_CONN_ID)],
                                [AZURE_CONN]
                            )
                        ])
                        mock_hdi_hook.return_value.get_cluster_state.return_value = ClusterGetProperties(
                            cluster_definition=None,
                            connectivity_endpoints=[ConnectivityEndpoint(
                                name='SSH',
                                location=ssh_endpoint,
                                port=ssh_port
                            )]
                        )
                        mock_hdi_hook.return_value.get_connection.return_value = AZURE_CONN
                        op = AzureHDInsightSshOperator(
                                                     command='date',
                                                     cluster_name=CLUSTER_NAME,
                                                     azure_conn_id=AZURE_CONN_ID,
                                                     task_id='foo',
                                                     dag=self.dag)
                        op.execute(None)
                        ssh_hook.assert_called_once_with(
                            remote_host=ssh_endpoint,
                            port=ssh_port,
                            username=AZURE_CONN.extra_dejson['SSH_USER_NAME'],
                            password=AZURE_CONN.extra_dejson['SSH_PASSWORD']
                        )
                        mock_ssh_op_exec.assert_called_once()