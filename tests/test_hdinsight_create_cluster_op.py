import unittest

from airflow import DAG
from mock import patch

from airflowhdi.operators import AzureHDInsightCreateClusterOperator
from tests.test_commons import AZURE_CONN_ID, CLUSTER_NAME, CLUSTER_PARAMS, DEFAULT_DAG_ARGS


class TestHDinsightCreateClusterOp(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    def test_execute(self):
        with patch('airflowhdi.operators.azure_hdinsight_create_cluster_operator.AzureHDInsightHook') as mock_hook:
            op = AzureHDInsightCreateClusterOperator(cluster_name=CLUSTER_NAME,
                                                     cluster_params=CLUSTER_PARAMS,
                                                     azure_conn_id=AZURE_CONN_ID,
                                                     task_id='foo',
                                                     dag=self.dag)
            op.execute(None)
            mock_hook.return_value.create_cluster.assert_called_once()