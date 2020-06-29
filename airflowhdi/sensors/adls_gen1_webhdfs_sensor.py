from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AzureDataLakeStorageGen1WebHdfsSensor(BaseSensorOperator):
    """
    Waits for blobs matching a wildcard prefix to arrive on Azure Data Lake Storage.
    """
    template_fields = ('glob_path',)  # type: Iterable[str]
    ui_color = '#901dd2'

    @apply_defaults
    def __init__(self,
                 glob_path,
                 azure_data_lake_conn_id='azure_data_lake_default',
                 *args,
                 **kwargs):
        """
        :param glob_path: glob path, allows wildcards
        :param azure_data_lake_conn_id: connection reference to ADLS
        """
        super(AzureDataLakeStorageGen1WebHdfsSensor, self).__init__(*args, **kwargs)
        self.glob_path = glob_path
        self.azure_data_lake_conn_id = azure_data_lake_conn_id

    def poke(self, context):
        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id
        )
        adls_conn = hook.get_conn()
        self.log.info('Poking for glob path: %s in ADLS://%s', self.glob_path, adls_conn.kwargs['store_name'])

        return hook.check_for_file(self.glob_path)