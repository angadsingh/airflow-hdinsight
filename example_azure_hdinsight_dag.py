from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow import DAG, AirflowException
from airflow.utils import dates
from airflow.utils.trigger_rule import TriggerRule

from airflowhdi.operators.azure_hdinsight_create_cluster_operator \
    import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators.azure_hdinsight_delete_cluster_operator \
    import AzureHDInsightDeleteClusterOperator
from airflowhdi.operators import LivyBatchOperator
from airflowhdi.sensors.livy_batch_sensor import LivyBatchSensor
from airflowhdi.sensors.azure_hdinsight_cluster_sensor import AzureHDInsightClusterSensor

default_args = {
    'owner': 'angad',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    "retries": 0
}

cluster_name = "airflowtesthdi"

AZURE_CONN_ID = 'azure_hdi_default'
HDI_CONN_ID = 'azure_hdi_cluster_params_default'


def handle_failure_task():
    raise AirflowException('Marking DAG as failed due to an upstream failure!')


with DAG(dag_id='example_azure_hdinsight_dag',
         default_args=default_args,
         dagrun_timeout=timedelta(hours=2),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False,
         doc_md=__doc__) as dag:

    create_cluster_op = ConnectedAzureHDInsightCreateClusterOperator(task_id="start_cluster",
                                                                     azure_conn_id=AZURE_CONN_ID,
                                                                     hdi_conn_id=HDI_CONN_ID,
                                                                     cluster_name=cluster_name,
                                                                     trigger_rule=TriggerRule.ALL_SUCCESS)

    monitor_cluster_provisioning_op = AzureHDInsightClusterSensor(cluster_name,
                                                                  azure_conn_id=AZURE_CONN_ID,
                                                                  task_id='hdi_provisioning_sensor',
                                                                  poke_interval=5,
                                                                  provisioning_only=True)

    monitor_cluster_op = AzureHDInsightClusterSensor(cluster_name,
                                                     azure_conn_id=AZURE_CONN_ID,
                                                     task_id='hdi_cluster_sensor',
                                                     poke_interval=5)

    handle_failure_op = PythonOperator(
        task_id='handle_failure',
        python_callable=handle_failure_task,
        trigger_rule=TriggerRule.ONE_FAILED)

    livy_submit = LivyBatchOperator(
        task_id='livy_submit',
        name="batch_example_{{ run_id }}",
        file='wasb:///example/jars/spark-examples.jar',
        arguments=[10],
        num_executors=1,
        azure_conn_id=AZURE_CONN_ID,
        cluster_name=cluster_name,
        conf={
            'spark.shuffle.compress': 'false',
        },
        class_name='org.apache.spark.examples.SparkPi',
        proxy_user='admin',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(minutes=10)
    )

    livy_sensor = LivyBatchSensor(
        batch_id="{{ task_instance.xcom_pull('livy_submit', key='return_value') }}",
        task_id='livy_sensor',
        azure_conn_id=AZURE_CONN_ID,
        cluster_name=cluster_name,
        verify_in="yarn",
        poke_interval=20,
        timeout=600,
    )

    terminate_cluster_op = AzureHDInsightDeleteClusterOperator(task_id="delete_cluster",
                                                               azure_conn_id=AZURE_CONN_ID,
                                                               cluster_name=cluster_name,
                                                               trigger_rule=TriggerRule.ALL_DONE)

    create_cluster_op >> monitor_cluster_provisioning_op >> monitor_cluster_op >> handle_failure_op
    monitor_cluster_provisioning_op >> livy_submit >> livy_sensor >> terminate_cluster_op

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()