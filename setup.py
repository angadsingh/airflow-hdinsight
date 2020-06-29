from setuptools import setup

setup(
    name='airflow-hdinsight',
    version='0.0.1',
    author="Angad Singh",
    author_email="angad.singh@trufactor.io",
    description="HDInsight provider for Airflow",
    long_description="""A set of airflow hooks, operators and sensors to allow airflow DAGs
        to operate with the Azure HDInsight platform, for cluster creation and monitoring
        as well as job submission and monitoring.""",
    license="MIT",
    url="https://gitlab.pinsightmedia.com/telco-dmp/airflow-hdinsight",
    py_modules=['airflowhdi'],
    install_requires=[
        'azure-mgmt-hdinsight~=1.5.1',
        'msrestazure~=0.6.3',
        'apache-airflow>=1.10.10,<=2.*',
        'azure-storage-blob==2.1.0',
        'azure-storage-common==2.1.0',
        'azure-storage-nspkg==3.1.0',
        'azure-datalake-store',
        'paramiko',
        'sshtunnel'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)