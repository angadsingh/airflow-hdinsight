from airflow.hooks.base_hook import BaseHook
from json import JSONDecodeError
from numbers import Number
import json
from airflow.exceptions import AirflowBadRequest, AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow import settings
from airflow.models import Connection

LIVY_ENDPOINT = "batches"
LOG_PAGE_LINES = 100
SPARK_ENDPOINT = "api/v1/applications"
YARN_ENDPOINT = "ws/v1/cluster/apps"
VERIFICATION_METHODS = ["spark", "yarn"]


class LivyBatchHook(BaseHook):
    template_fields = ["file", "proxy_user", "class_name", "arguments", "jars", "py_files", "files", "driver_memory",
                       "driver_cores", "executor_memory", "executor_cores", "num_executors", "archives", "queue", "name",
                       "conf", "azure_conn_id", "cluster_name", "batch_id"]

    class LocalConnHttpHook(HttpHook):
        def __init__(self, batch_hook, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.batch_hook = batch_hook

        def get_connection(self, conn_id):
            if conn_id == 'livy_conn_id':
                return self.batch_hook.livy_conn
            if conn_id == 'spark_conn_id':
                return self.batch_hook.spark_conn
            if conn_id == 'yarn_conn_id':
                return self.batch_hook.yarn_conn

    def __init__(self, file=None, proxy_user=None, class_name=None, arguments=None, jars=None, py_files=None,
                 files=None, driver_memory=None, driver_cores=None, executor_memory=None, executor_cores=None,
                 num_executors=None, archives=None, queue=None, name=None, conf=None, azure_conn_id=None,
                 cluster_name=None, batch_id=None, verify_in=None):

        super().__init__(source=None)
        self.file = file
        self.proxy_user = proxy_user
        self.class_name = class_name
        self.arguments = arguments
        self.jars = jars
        self.py_files = py_files
        self.files = files
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.archives = archives
        self.queue = queue
        self.name = name
        self.conf = conf
        self.azure_conn_id = azure_conn_id
        self.cluster_name = cluster_name
        self.batch_id = batch_id
        self.verify_in = verify_in
        self.connections_created = False

    def create_livy_connections(self):
        """"Creates a livy connection dynamically"""
        session = settings.Session()
        azure_conn = session.query(Connection).filter(Connection.conn_id == self.azure_conn_id).first()

        if not azure_conn:
            raise AirflowException(f"Azure connection not found: {self.azure_conn_id}")

        username = azure_conn.extra_dejson['CLUSTER_LOGIN_USER_NAME']
        password = azure_conn.extra_dejson['CLUSTER_PASSWORD']

        self.livy_conn = Connection(conn_id='livy_conn_id')
        self.livy_conn.login = username
        self.livy_conn.set_password(password)
        self.livy_conn.schema = 'https'
        self.livy_conn.extra = f"{{ \"X-Requested-By\": \"{username}\" }}"
        self.livy_conn.host = f"https://{self.cluster_name}.azurehdinsight.net/livy"

        self.spark_conn = Connection(conn_id='spark_conn_id')
        self.spark_conn.login = username
        self.spark_conn.set_password(password)
        self.spark_conn.schema = 'https'
        self.spark_conn.extra = f"{{ \"X-Requested-By\": \"{username}\" }}"
        self.spark_conn.host = f"https://{self.cluster_name}.azurehdinsight.net/sparkhistory"

        self.yarn_conn = Connection(conn_id='yarn_conn_id')
        self.yarn_conn.login = username
        self.yarn_conn.set_password(password)
        self.yarn_conn.schema = 'https'
        self.yarn_conn.extra = f"{{ \"X-Requested-By\": \"{username}\" }}"
        self.yarn_conn.host = f"https://{self.cluster_name}.azurehdinsight.net/yarnui"

        self.connections_created = True

    def submit_batch(self):
        if not self.connections_created:
            self.create_livy_connections()
        headers = {"X-Requested-By": "airflow", "Content-Type": "application/json"}
        unfiltered_payload = {
            "file": self.file,
            "proxyUser": self.proxy_user,
            "className": self.class_name,
            "args": self.arguments,
            "jars": self.jars,
            "pyFiles": self.py_files,
            "files": self.files,
            "driverMemory": self.driver_memory,
            "driverCores": self.driver_cores,
            "executorMemory": self.executor_memory,
            "executorCores": self.executor_cores,
            "numExecutors": self.num_executors,
            "archives": self.archives,
            "queue": self.queue,
            "name": self.name,
            "conf": self.conf,
        }
        payload = {k: v for k, v in unfiltered_payload.items() if v}
        self.log.info(
            f"Submitting the batch to Livy... "
            f"Payload:\n{json.dumps(payload, indent=2)}"
        )
        response = self.LocalConnHttpHook(self, http_conn_id='livy_conn_id').run(
            LIVY_ENDPOINT, json.dumps(payload), headers
        )

        try:
            batch_id = json.loads(response.content)["id"]
        except (JSONDecodeError, LookupError) as ex:
            self._log_response_error("$.id", response)
            raise AirflowBadRequest(ex)

        if not isinstance(batch_id, Number):
            raise AirflowException(
                f"ID of the created batch is not a number ({batch_id}). "
                "Are you sure we're calling Livy API?"
            )
        self.batch_id = batch_id
        self.log.info(f"Batch successfully submitted with id %s", self.batch_id)
        return self.batch_id

    def get_batch_state(self):
        if not self.connections_created:
            self.create_livy_connections()
        self.log.info("Getting batch %s status...", self.batch_id)
        endpoint = f"{LIVY_ENDPOINT}/{self.batch_id}"
        response = self.LocalConnHttpHook(self, method="GET", http_conn_id='livy_conn_id').run(endpoint)
        try:
            return json.loads(response.content)["state"]
        except (JSONDecodeError, LookupError) as ex:
            self._log_response_error("$.state", response, self.batch_id)
            raise AirflowBadRequest(ex)

    def verify(self):
        if not self.connections_created:
            self.create_livy_connections()
        app_id = self._get_spark_app_id(self.batch_id)
        if app_id is None:
            raise AirflowException(f"Spark appId was null for batch {self.batch_id}")
        self.log.info("Found app id '%s' for batch id %s.", app_id, self.batch_id)
        if self.verify_in == "spark":
            self._check_spark_app_status(app_id)
        else:
            self._check_yarn_app_status(app_id)
        self.log.info("App '%s' associated with batch %s completed!",
                      app_id, self.batch_id)

    def _get_spark_app_id(self, batch_id):
        self.log.info("Getting Spark app id from Livy API for batch %s...", batch_id)
        endpoint = f"{LIVY_ENDPOINT}/{batch_id}"
        response = self.LocalConnHttpHook(self, method="GET", http_conn_id='livy_conn_id').run(
            endpoint
        )
        try:
            return json.loads(response.content)["appId"]
        except (JSONDecodeError, LookupError, AirflowException) as ex:
            self._log_response_error("$.appId", response, batch_id)
            raise AirflowBadRequest(ex)

    def _check_spark_app_status(self, app_id):
        self.log.info("Getting app status (id=%s) from Spark REST API...", app_id)
        endpoint = f"{SPARK_ENDPOINT}/{app_id}/jobs"
        response = self.LocalConnHttpHook(self, method="GET", http_conn_id='spark_conn_id').run(
            endpoint
        )
        try:
            jobs = json.loads(response.content)
            expected_status = "SUCCEEDED"
            for job in jobs:
                job_id = job["jobId"]
                job_status = job["status"]
                self.log.info(
                    "Job id %s associated with application '%s' is '%s'",
                    job_id, app_id, job_status
                )
                if job_status != expected_status:
                    raise AirflowException(
                        f"Job id '{job_id}' associated with application '{app_id}' "
                        f"is '{job_status}', expected status is '{expected_status}'"
                    )
        except (JSONDecodeError, LookupError, TypeError) as ex:
            self._log_response_error("$.jobId, $.status", response)
            raise AirflowBadRequest(ex)

    def _check_yarn_app_status(self, app_id):
        self.log.info("Getting app status (id=%s) from YARN RM REST API...", app_id)
        endpoint = f"{YARN_ENDPOINT}/{app_id}"
        response = self.LocalConnHttpHook(self, method="GET", http_conn_id='yarn_conn_id').run(
            endpoint
        )
        try:
            status = json.loads(response.content)["app"]["finalStatus"]
        except (JSONDecodeError, LookupError, TypeError) as ex:
            self._log_response_error("$.app.finalStatus", response)
            raise AirflowBadRequest(ex)
        expected_status = "SUCCEEDED"
        if status != expected_status:
            raise AirflowException(
                f"YARN app {app_id} is '{status}', expected status: '{expected_status}'"
            )

    def spill_batch_logs(self):
        if not self.connections_created:
            self.create_livy_connections()
        dashes = 50
        self.log.info(f"{'-'*dashes}Full log for batch %s{'-'*dashes}", self.batch_id)
        endpoint = f"{LIVY_ENDPOINT}/{self.batch_id}/log"
        hook = self.LocalConnHttpHook(self, method="GET", http_conn_id='livy_conn_id')
        line_from = 0
        line_to = LOG_PAGE_LINES
        while True:
            log_page = self._fetch_log_page(hook, endpoint, line_from, line_to)
            try:
                logs = log_page["log"]
                for log in logs:
                    self.log.info(log.replace("\\n", "\n"))
                actual_line_from = log_page["from"]
                total_lines = log_page["total"]
            except LookupError as ex:
                self._log_response_error("$.log, $.from, $.total", log_page)
                raise AirflowBadRequest(ex)
            actual_lines = len(logs)
            if actual_line_from + actual_lines >= total_lines:
                self.log.info(
                    f"{'-' * dashes}End of full log for batch %s"
                    f"{'-' * dashes}", self.batch_id
                )
                break
            line_from = actual_line_from + actual_lines

    def _fetch_log_page(self, hook: LocalConnHttpHook, endpoint, line_from, line_to):
        prepd_endpoint = endpoint + f"?from={line_from}&size={line_to}"
        response = hook.run(prepd_endpoint)
        try:
            return json.loads(response.content)
        except JSONDecodeError as ex:
            self._log_response_error("$", response)
            raise AirflowBadRequest(ex)

    def close_batch(self):
        self.log.info(f"Closing batch with id = %s", self.batch_id)
        batch_endpoint = f"{LIVY_ENDPOINT}/{self.batch_id}"
        self.LocalConnHttpHook(self, method="DELETE", http_conn_id='livy_conn_id').run(
            batch_endpoint
        )
        self.log.info(f"Batch %s has been closed", self.batch_id)

    def _log_response_error(self, lookup_path, response, batch_id=None):
        msg = "Can not parse JSON response."
        if batch_id is not None:
            msg += f" Batch id={batch_id}."
        try:
            pp_response = (
                json.dumps(json.loads(response.content), indent=2)
                if "application/json" in response.headers.get("Content-Type", "")
                else response.content
            )
        except AttributeError:
            pp_response = json.dumps(response, indent=2)
        msg += f"\nTried to find JSON path: {lookup_path}, but response was:\n{pp_response}"
        self.log.error(msg)