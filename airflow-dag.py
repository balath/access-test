import pathlib
from datetime import datetime
 
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
 
metadataPath = "/tmp/metadata.json"
metadataUrl = "https://raw.githubusercontent.com/balath/sdg-access-test/main/metadata/metadata.json?token=GHSAT0AAAAAABVSTSZGEMNFRVF2IIWFBDKQYVMGFQQ"


dag = DAG(
   dag_id="sdg-access-test",
   schedule_interval=None,
   start_date=datetime(2022, 6, 17),
)
 
download_metadata = BashOperator(
   task_id="download_metadata",
   bash_command= f"curl -o {metadataPath} -L '{metadataUrl}'",
   dag=dag,
)
 
 
submit_job = SparkSubmitOperator(
        conf = 
        application = "./dags/sdg-access-test.jar", 
        application_args = [metadataPath]
        task_id="submit_job"
    )
  
download_metadata >> submit_job