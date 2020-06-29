from airflow import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflowhdi.hooks import LivyBatchHook

VERIFICATION_METHODS = ["spark", "yarn"]
NON_TERMINAL_BATCH_STATES = [
    "not_started",
    "starting",
    "recovering",
    "idle",
    "running",
    "busy",
    "shutting_down",
]


class LivyBatchSensor(BaseSensorOperator):
    """
    Monitors a spark job submitted through Livy, uptil a terminal state is achieved.
    And then optionally does additional verification as well as printing of the logs

    .. seealso::
        See the documentation of :class:`airflowhdi.hooks.LivyBatchHook`
    """
    template_fields = ['batch_id']

    def __init__(
        self,
        batch_id,
        verify_in=None,
        spill_logs=True,
        azure_conn_id=None,
        cluster_name=None,
        mode="poke",
        poke_interval=20,
        *args,
        **kwargs
    ):
        """
        :param cluster_name: name of the cluster to check the state of
        :type cluster_name: str
        :param azure_conn_id: azure connection to get config from
        :type azure_conn_id: str

        :param batch_id: batch ID of the livy spark job to monitor
        :type batch_id: string
        :param verify_in: Specify the additional verification method. Either `spark` or `yarn`
        :type verify_in: string
        :param spill_logs: whether or not to spill the logs of the batch job using livy's batch API
        :type spill_logs: bool
        """
        super().__init__(
            mode=mode,
            poke_interval=poke_interval,
            *args,
            **kwargs
        )
        if verify_in in VERIFICATION_METHODS or verify_in is None:
            self.verify_in = verify_in
        else:
            raise AirflowException(
                f"Can not create batch operator with verification method "
                f"'{verify_in}'!\nAllowed methods: {VERIFICATION_METHODS}"
            )

        self.spill_logs = spill_logs
        self.batch_id = batch_id
        self.azure_conn_id = azure_conn_id
        self.cluster_name = cluster_name

    def poke(self, context):
        batch_hook = LivyBatchHook(batch_id=self.batch_id,
                                   azure_conn_id=self.azure_conn_id,
                                   cluster_name=self.cluster_name,
                                   verify_in=self.verify_in)

        state = batch_hook.get_batch_state()

        if state in NON_TERMINAL_BATCH_STATES:
            self.log.info(
                "Batch %s has not finished yet (state is '%s')", self.batch_id, state)
            return False
        if state == "success":
            self.log.info(f"Batch {self.batch_id} has finished successfully!")
            if self.verify_in in VERIFICATION_METHODS:
                self.log.info(
                    f"Additionally verifying status for batch id {self.batch_id} "
                    f"via {self.verify_in}..."
                )
                batch_hook.verify()

        if self.spill_logs:
            batch_hook.spill_batch_logs()
        batch_hook.close_batch()

        if state != "success":
            raise AirflowException(f"Batch {self.batch_id} failed with state '{state}'")
        else:
            return True

