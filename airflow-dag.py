from datetime import datetime
 
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator #Airflow 1.10
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator #Airflow 2
 
metadataPath = "/metadata.json"
metadataUrl = "https://raw.githubusercontent.com/balath/access-test/main/metadata/metadata.json?token=GHSAT0AAAAAABVSTSZGEMNFRVF2IIWFBDKQYVMGFQQ"

dag = DAG(
    dag_id="access-test",
    schedule_interval=None,
    start_date=datetime(2022, 6, 17),
)

download_metadata = BashOperator(
    task_id="download_metadata",
    bash_command= f"curl -o {metadataPath} -L '{metadataUrl}'",
    dag=dag,
)

submit_job = SparkSubmitOperator(
    application = "${SPARK_HOME}/access-test-app.jar",
    application_args = [metadataPath],
    task_id="submit_job"
)

download_metadata >> submit_job