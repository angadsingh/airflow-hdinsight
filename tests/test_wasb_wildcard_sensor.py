import unittest
from collections import namedtuple

from parameterized import parameterized
from airflow import DAG
from mock import patch, create_autospec

from airflowhdi.sensors import WasbWildcardPrefixSensor
from tests.test_commons import DEFAULT_DAG_ARGS

Key = namedtuple('BlobKey', ['name'])

class TestWasbWildcardPrefixSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag_id', default_args=DEFAULT_DAG_ARGS)

    @parameterized.expand([
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/foe*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/foe/*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/fum/*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/fum/1*', False),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foo/fum/2*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '/foe/*', False),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '*', True),
        ('test_container_1', [Key('/foo/foe/1.part'), Key('/foo/fum/2.part')], '_*', False)
    ])
    def test_poke(self, container_name, key_list, wildcard_prefix, match):
        def mock_list_blobs(self, *args, **kwargs):
            return key_list

        with patch('airflowhdi.sensors.wasb_wildcard_sensor.WasbHook') as mock_wasb_hook:
            sensor = WasbWildcardPrefixSensor(
                wasb_conn_id='test_conn_id',
                container_name=container_name,
                wildcard_prefix=wildcard_prefix,
                check_options={},
                task_id='foo',
                dag=self.dag
            )
            mock_wasb_hook.return_value.connection.list_blobs = mock_list_blobs
            poke_result = sensor.poke(None)
            mock_wasb_hook.assert_called_once_with(wasb_conn_id='test_conn_id')
            self.assertEqual(match, poke_result)

