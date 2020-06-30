# airflow-hdinsight

[![Documentation Status](https://readthedocs.org/projects/docs/badge/?version=latest)](https://airflow-hdinsight.readthedocs.io/en/latest/)

A set of airflow hooks, operators and sensors to allow airflow DAGs to operate with the Azure HDInsight platform, for cluster creation and monitoring as well as job submission and monitoring. Also included are some enhanced Azure Blob and Data Lake sensors.

This project is both an amalgamation and enhancement of existing open source airflow extensions, plus new extensions to solve the problem.

##### Installation

`pip install airflow-hdinsight`

##### Extensions

| Type     | Name                                         | What it does                                                 |
| -------- | -------------------------------------------- | ------------------------------------------------------------ |
| Hook     | AzureHDInsightHook                           | Uses the HDInsightManagementClient from the [HDInsight SDK for Python](https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python) to expose several operations on an HDInsight cluster - get cluster state, create, delete. |
| Operator | AzureHDInsightCreateClusterOperator          | Use the AzureHDInsightHook to create a cluster               |
| Operator | AzureHDInsightDeleteClusterOperator          | Use the AzureHDInsightHook to delete a cluster               |
| Operator | ConnectedAzureHDInsightCreateClusterOperator | Extends the AzureHDInsightCreateClusterOperator to allow fetching of the security credentials and cluster creation spec from an airflow connection |
| Operator | AzureHDInsightSshOperator                    | Uses the AzureHDInsightHook and SSHHook to run an SSH command on the master node of the given HDInsight cluster |
| Sensor   | AzureHDInsightClusterSensor                  | A sensor to monitor the provisioning state or running state (can switch between either mode) of a given HDInsight cluster. Uses the AzureHDInsightHook. |
| Hook     | LivyBatchHook                                | Uses the Apache Livy [Batch API](https://livy.incubator.apache.org/docs/latest/rest-api.html) to submit spark jobs to a livy server, get batch state, verify batch state by quering either the spark history server or yarn resource manager, spill the logs of the spark job post completion, etc. |
| Operator | LivyBatchOperator                            | Uses the LivyBatchHook to submit a spark job to a livy server |
| Sensor   | LivyBatchSensor                              | Uses the LivyBatchHook to sense termination and verify completion, spill logs of a spark job submitted earlier to a livy server |
| Sensor   | WasbWildcardPrefixSensor                     | An enhancement to the [WasbPrefixSensor](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/sensors/wasb.py#L62) to support sensing on a wildcard prefix |
| Sensor   | AzureDataLakeStorageGen1WebHdfsSensor        | Uses airflow's [AzureDataLakeHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/azure/hooks/azure_data_lake.py) to sense a glob path (which implicitly supports wildcards) on ADLS Gen 1. ADLS Gen 2 is not yet supported in airflow. |

**Origins of the HDinsight operator work**

The HDInsight operator work is loosely inspired from  [alikemalocalan/airflow-hdinsight-operators](alikemalocalan/airflow-hdinsight-operators), however that has a huge number of defects, as to why it was [never accepted](https://issues.apache.org/jira/browse/AIRFLOW-3604) to be [merged](https://github.com/apache/airflow/pull/4460) into airflow in the first place. This project solves all of those issues and more, and is frankly a full rewrite.

**Origins of the livy work**

The livy batch operator is based on the work by [panovvv](https://github.com/panovvv)'s project [airfllow-livy-operators](https://github.com/panovvv/airflow-livy-operators). It does some necessary changes:

- Seperates the operator into a hook (LivyBatchHook), an operator (LivyBatchOperator) and a sensor (LivyBatchSensor)
- Adds additional verification and log spilling to the sensor (the original sensor does not)
- Removes additional verifiation and log spilling from the operator - hence alllowing a async pattern akin to the EMR add step operator and step sensor.
- Creates livy, spark and YARN airflow connections dynamically from an Azure HDInsight connection
- Returns the batch ID from the operator so that a sensor can use it after being passed through XCom
- Changes logging to LoggingMixin calls
- Allows templatization of fields

**State of airflow livy operators in the wild..**

As it stands today (June of 2020), there are multiple airflow livy operator projects out there:

- [panovvv/airflow-livy-operators](panovvv/airflow-livy-operators): the project which this project bases its work on
- the [official livy provider](https://github.com/apache/airflow/tree/master/airflow/providers/apache/livy) in airflow 2.0, with a backport available for airflow 1.1.x: alas the official provider has very limited functionality - it does not spill the job's logs, and it does not do additional verification for job completion using spark history server or yarn resource manager, amongst other limitations
- [rssanders3/airflow-spark-operator-plugin](rssanders3/airflow-spark-operator-plugin): this is the oldest livy operator, which only supports livy sessions and not batches. there's a copy of this in [alikemalocalan/airflow-hdinsight-operators](alikemalocalan/airflow-hdinsight-operators).