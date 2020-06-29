from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflowhdi.hooks import LivyBatchHook


class LivyBatchOperator(BaseOperator):
    """
    Quoted from `airflow-livy-operators <https://github.com/panovvv/airflow-livy-operators>`_:

    .. highlight:: none

    ::

        Workflow
        1. Submit a batch to Livy.
        2. Poll API until it's ready.
        3. If an additional verification method is specified, retrieve the job status
            there and disregard the batch status from Livy.

        Supported verification methods are Spark/YARN REST API.
        When the batch job is running in cluster mode on YARN cluster,
        it sometimes shows as "succeeded" even when underlying job fails.

    .. seealso::
        * https://livy.incubator.apache.org/docs/latest/rest-api.html
        * https://spark.apache.org/docs/latest/monitoring.html#rest-api
        * https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
    """
    template_fields = ["name", "arguments", "batch_hook"]

    @apply_defaults
    def __init__(
        self,
        file=None,
        proxy_user=None,
        class_name=None,
        arguments=None,
        jars=None,
        py_files=None,
        files=None,
        driver_memory=None,
        driver_cores=None,
        executor_memory=None,
        executor_cores=None,
        num_executors=None,
        archives=None,
        queue=None,
        name=None,
        conf=None,
        azure_conn_id=None,
        cluster_name=None,
        *args,
        **kwargs
    ):
        """
        .. seealso::
            See the documentation of :class:`airflowhdi.hooks.LivyBatchHook`
            for explanation on the parameters of this operator
        """
        super(LivyBatchOperator, self).__init__(*args, **kwargs)
        self.name = name
        self.arguments = arguments
        self.batch_hook = LivyBatchHook(
            file=file,
            proxy_user=proxy_user,
            class_name=class_name,
            arguments=self.arguments,
            jars=jars,
            py_files=py_files,
            files=files,
            driver_memory=driver_memory,
            driver_cores=driver_cores,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            num_executors=num_executors,
            archives=archives,
            queue=queue,
            name=self.name,
            conf=conf,
            azure_conn_id=azure_conn_id,
            cluster_name=cluster_name
        )

    def execute(self, context):
        return self.batch_hook.submit_batch()
