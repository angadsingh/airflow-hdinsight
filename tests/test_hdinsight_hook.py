import unittest
from unittest import mock

from airflow.models import Connection
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from msrestazure.azure_active_directory import AdalAuthentication

from airflowhdi.hooks import AzureHDInsightHook
from tests.test_commons import AZURE_CONN, AZURE_CONN_ID, CLUSTER_NAME, CLUSTER_PARAMS

class TestAzureHDinsightHook(unittest.TestCase):

    def setUp(self) -> None:
        self.session = UnifiedAlchemyMagicMock(data=[
            (
                [mock.call.query(Connection),
                 mock.call.filter(Connection.conn_id == AZURE_CONN_ID)],
                [AZURE_CONN]
            )])

    @mock.patch('airflow.settings.Session')
    def test_init(self, mock_session):
        mock_session.return_value = self.session
        hook = AzureHDInsightHook(azure_conn_id=AZURE_CONN_ID)

        self.assertEqual(hook.resource_group_name,
                         AZURE_CONN.extra_dejson['resource_group_name'])
        self.assertEqual(hook.resource_group_location,
                         AZURE_CONN.extra_dejson['resource_group_location'])
        self.assertEqual(hook.client.config.subscription_id,
                         AZURE_CONN.extra_dejson['servicePrincipal']['subscriptionId'])
        self.assertIsInstance(hook.client.config.credentials, AdalAuthentication)
        cred: AdalAuthentication = hook.client.config.credentials
        self.assertEqual(hook.client.config.credentials._args[1],
                         AZURE_CONN.extra_dejson['servicePrincipal']['clientId'])
        self.assertEqual(hook.client.config.credentials._args[2],
                         AZURE_CONN.extra_dejson['servicePrincipal']['clientSecret'])

    def test_create_cluster(self):
        with mock.patch('airflow.settings.Session') as mock_session:
            with mock.patch.object(AzureHDInsightHook, 'get_conn') as mock_get_conn:
                mock_session.return_value = self.session
                hook = AzureHDInsightHook(azure_conn_id=AZURE_CONN_ID)
                hook.create_cluster(CLUSTER_PARAMS, CLUSTER_NAME)
                mock_get_conn.return_value.clusters.create.assert_called_once()
                args, kwargs = mock_get_conn.return_value.clusters.create.call_args
                self.assertEqual(kwargs['cluster_name'], CLUSTER_NAME)
                self.assertEqual(kwargs['resource_group_name'], AZURE_CONN.extra_dejson['resource_group_name'])
                self.assertEqual(kwargs['parameters'].location, AZURE_CONN.extra_dejson['resource_group_location'])
                self.assertEqual(kwargs['parameters'].properties, CLUSTER_PARAMS)

    def test_delete_cluster(self):
        with mock.patch('airflow.settings.Session') as mock_session:
            with mock.patch.object(AzureHDInsightHook, 'get_conn') as mock_get_conn:
                mock_session.return_value = self.session
                hook = AzureHDInsightHook(azure_conn_id=AZURE_CONN_ID)
                hook.delete_cluster(CLUSTER_NAME)
                mock_get_conn.return_value.clusters.delete.assert_called_once_with(
                    AZURE_CONN.extra_dejson['resource_group_name'],
                    cluster_name=CLUSTER_NAME
                )
                mock_get_conn.return_value.clusters.delete.return_value.wait.assert_called_once()

    def test_get_cluster_state(self):
        with mock.patch('airflow.settings.Session') as mock_session:
            with mock.patch.object(AzureHDInsightHook, 'get_conn') as mock_get_conn:
                mock_session.return_value = self.session
                hook = AzureHDInsightHook(azure_conn_id=AZURE_CONN_ID)
                hook.get_cluster_state(CLUSTER_NAME)
                mock_get_conn.return_value.clusters.get.assert_called_once_with(
                    AZURE_CONN.extra_dejson['resource_group_name'],
                    CLUSTER_NAME
                )