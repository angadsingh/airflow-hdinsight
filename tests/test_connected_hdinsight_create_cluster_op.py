import unittest

from airflow import DAG
from airflow.models import Connection
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from mock import patch, call

from airflowhdi.operators import ConnectedAzureHDInsightCreateClusterOperator
from tests.test_commons import AZURE_CONN, AZURE_CONN_ID, HDI_CONN, HDI_CONN_ID, CLUSTER_NAME, CLUSTER_PARAMS, DEFAULT_DAG_ARGS


class TestConnectedHDinsightCreateClusterOp(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    def test_init(self):
        with patch('airflow.settings.Session') as mock_session:
            mock_session.return_value = UnifiedAlchemyMagicMock(data=[
                (
                    [call.query(Connection),
                     call.filter(Connection.conn_id == AZURE_CONN_ID)],
                    [AZURE_CONN]
                ),
                (
                    [call.query(Connection),
                     call.filter(Connection.conn_id == HDI_CONN_ID)],
                    [HDI_CONN]
                )
            ])
            op = ConnectedAzureHDInsightCreateClusterOperator(
                                                     cluster_name=CLUSTER_NAME,
                                                     azure_conn_id=AZURE_CONN_ID,
                                                     hdi_conn_id=HDI_CONN_ID,
                                                     task_id='foo',
                                                     dag=self.dag)

            self.assertEqual(op.cluster_params,  CLUSTER_PARAMS)
            self.assertEqual(op.params, AZURE_CONN.extra_dejson)