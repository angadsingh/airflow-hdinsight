import unittest

from airflow import DAG
from mock import patch

from airflowhdi.sensors import AzureDataLakeStorageGen1WebHdfsSensor
from tests.test_commons import DEFAULT_DAG_ARGS


class TestAzureDataLakeStorageGen1WebHdfsSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    def test_poke(self):
        with patch('airflowhdi.sensors.adls_gen1_webhdfs_sensor.AzureDataLakeHook') as mock_adls_hook:
            sensor = AzureDataLakeStorageGen1WebHdfsSensor(
                glob_path='test_path',
                azure_data_lake_conn_id='test_adls_conn',
                task_id='foo',
                dag=self.dag
            )
            sensor.poke(None)
            mock_adls_hook.return_value.check_for_file.assert_called_once_with('test_path')