import os

from airflow.models import Connection
from airflow.utils import dates
from azure.mgmt.hdinsight.models import ClusterCreateProperties, ClusterDefinition, ComputeProfile, Role, \
    LinuxOperatingSystemProfile, OsProfile, \
    StorageProfile, StorageAccount, OSType, Tier, HardwareProfile

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    "retries": 0,
}

AZURE_CONN_ID = 'azure_hdi_default'
HDI_CONN_ID = 'azure_hdi_cluster_params_default'
CLUSTER_NAME = 'test_cluster'

with open(f"{os.path.dirname(__file__)}/resources/test_azure_conn.json") as f:
    AZURE_CONN = Connection(extra=f.read())

with open(f"{os.path.dirname(__file__)}/resources/test_hdi_conn_extra.py") as f:
    HDI_CONN = Connection(extra=f.read())

CLUSTER_PARAMS = eval(compile(HDI_CONN.extra, 'file', 'eval'))