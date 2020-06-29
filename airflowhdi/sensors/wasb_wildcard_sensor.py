import fnmatch
import re

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class WasbWildcardPrefixSensor(BaseSensorOperator):
    """
    Waits for blobs matching a wildcard prefix to arrive on Azure Blob Storage.

    :param container_name: Name of the container.
    :type container_name: str
    :param wildcard_prefix: Prefix of the blob. Allows wildcards.
    :type wildcard_prefix: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_prefix()` takes.
    :type check_options: dict

    .. seealso::
        See the documentation of :class:`airflow.contrib.sensors.wasb_sensor.WasbBlobSensor`
    """

    template_fields = ('container_name', 'wildcard_prefix')

    @apply_defaults
    def __init__(self, container_name, wildcard_prefix, wasb_conn_id='wasb_default',
                 check_options=None, *args, **kwargs):
        super(WasbWildcardPrefixSensor, self).__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.wildcard_prefix = wildcard_prefix
        self.check_options = check_options

    def poke(self, context):
        self.log.info('Poking for wildcard prefix: %s in wasb://%s', self.wildcard_prefix, self.container_name)
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        prefix = re.split(r'[*]', self.wildcard_prefix, 1)[0]
        klist = hook.connection.list_blobs(self.container_name, prefix,
                                             num_results=1, **self.check_options)
        if klist:
            blob_matches = [k for k in klist if fnmatch.fnmatch(k.name, self.wildcard_prefix)]
            if blob_matches:
                return True
