import unittest

from airflow import DAG, AirflowException
from azure.mgmt.hdinsight.models import ClusterGetProperties, HDInsightClusterProvisioningState
from mock import patch
from parameterized import parameterized

from airflowhdi.sensors import AzureHDInsightClusterSensor
from tests.test_commons import DEFAULT_DAG_ARGS, CLUSTER_NAME, AZURE_CONN_ID


class TestAzureHDInsightClusterSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    @parameterized.expand([
        (HDInsightClusterProvisioningState.in_progress, True, False, False),
        (HDInsightClusterProvisioningState.deleting, True, False, False),
        (HDInsightClusterProvisioningState.succeeded, True, True, False),
        (HDInsightClusterProvisioningState.canceled, True, None, True),
        (HDInsightClusterProvisioningState.failed, True, None, True),
        (HDInsightClusterProvisioningState.succeeded, False, False, False)
    ])
    def test_poke(self, provisioning_state, provisioning_only, expected_result, failed_state):
        with patch('airflowhdi.sensors.azure_hdinsight_cluster_sensor.AzureHDInsightHook') as mock_hdi_hook:
            mock_hdi_hook.return_value.get_cluster_state.return_value = ClusterGetProperties(
                cluster_definition=None,
                provisioning_state=provisioning_state,
                cluster_state='Starting'
            )

            op = AzureHDInsightClusterSensor(cluster_name=CLUSTER_NAME,
                                             azure_conn_id=AZURE_CONN_ID,
                                             provisioning_only=provisioning_only,
                                             task_id='foo',
                                             dag=self.dag)
            poke_result = None

            if failed_state:
                with self.assertRaises(AirflowException):
                    poke_result = op.poke(None)
            else:
                poke_result = op.poke(None)

            self.assertEqual(expected_result, poke_result)
