import logging
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# Dataset de rattrapage
rattrapage_dataset = Dataset("replay://rattrapage")

@task
def validate_rattrapage_payload():
    """
    Valide le JSON porté par le DatasetEvent.
    Format attendu :
    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }
    """
    ctx = get_current_context()
    events = ctx.get("dataset_events", [])

    if not events:
        raise ValueError("No DatasetEvent found for replay://rattrapage")

    dataset_event = events[0]
    payload = dataset_event.extra

    if not isinstance(payload, dict):
        raise ValueError("Dataset payload must be a JSON object")

    # contract_path
    contract_path = payload.get("contract_path")
    if not contract_path or not isinstance(contract_path, str):
        raise ValueError("'contract_path' must be a non-empty string")
    if not contract_path.startswith("/contracts/"):
        raise ValueError("'contract_path' must be under /contracts/")
    if not contract_path.endswith((".yml", ".yaml")):
        raise ValueError("'contract_path' must be a YAML file")

    # files
    files = payload.get("files")
    if not isinstance(files, list) or len(files) == 0:
        raise ValueError("'files' must be a non-empty list")
    for f in files:
        if not isinstance(f, str):
            raise ValueError("Each file must be a string")
        if not f.startswith("/raw/"):
            raise ValueError(f"File must be under /raw/: {f}")

    logger.info("Rattrapage dataset validated: contract=%s, %d files", contract_path, len(files))

    return {"contract_path": contract_path, "files": files}




import logging
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from config import NAMENODE

logger = logging.getLogger(__name__)

# Dataset de rattrapage
rattrapage_dataset = Dataset("replay://rattrapage")

@task
def validate_rattrapage_payload():
    ctx = get_current_context()
    events = ctx.get("dataset_events", [])

    if not events:
        raise ValueError("No DatasetEvent found for replay://rattrapage")

    dataset_event = events[0]
    payload = dataset_event.extra

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    contract_path = payload.get("contract_path")
    if not contract_path or not isinstance(contract_path, str):
        raise ValueError("'contract_path' must be a non-empty string")
    if not contract_path.startswith("/contracts/"):
        raise ValueError("'contract_path' must be under /contracts/")
    if not contract_path.endswith((".yml", ".yaml")):
        raise ValueError("'contract_path' must be a YAML file")

    files = payload.get("files")
    if not isinstance(files, list) or len(files) == 0:
        raise ValueError("'files' must be a non-empty list")
    for f in files:
        if not isinstance(f, str):
            raise ValueError("Each file must be a string")
        if not f.startswith("/raw/"):
            raise ValueError(f"File must be under /raw/: {f}")

    logger.info("Rattrapage Asset validated: contract=%s, %d files", contract_path, len(files))
    return {"contract_path": contract_path, "files": files}

# DAG déclenché par le Dataset
with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par Dataset replay://rattrapage",
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 1},
    schedule=[rattrapage_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rattrapage", "replay"],
) as dag:

    payload = validate_rattrapage_payload()

    @task
    def explode_files(payload):
        return [{"contract_path": payload["contract_path"], "file_path": f} for f in payload["files"]]

    files_to_process = explode_files(payload)

    @task_group
    def process_file(contract_path: str, file_path: str):

        @task
        def spark_validate_ingest(contract_path: str, file_path: str):
            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = random.randint(1000, 9999)
            job_name = f"rattrapage_{Path(file_path).stem}_{timestamp}_{random_suffix}"

            logger.info("Submitting Spark job %s", job_name)
            logger.info("Contract HDFS path: %s", contract_path)
            logger.info("Input HDFS file: %s", file_path)

            batch_id = livy_hook.post_batch(
                file=f"{NAMENODE}/scripts/check_meta_from_contract.py",
                name=job_name,
                args=[f"{NAMENODE}{contract_path}", f"{NAMENODE}{file_path}"],
                queue="default",
                conf={
                    "spark.sql.sources.partitionOverwriteMode": "dynamic",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.dynamicAllocation.enabled": "true",
                },
                driver_memory="1g",
                driver_cores=1,
                executor_memory="2g",
                executor_cores=2,
                num_executors=2,
            )

            final_state = livy_hook.poll_for_completion(session_id=batch_id, polling_interval=30, max_polling_attempts=120)
            logger.info("Spark job %s finished with state %s", batch_id, final_state.value)
            return {"batch_id": batch_id, "file": file_path, "state": final_state.value}

        spark_validate_ingest(contract_path, file_path)

    process_file.expand_kwargs(files_to_process)
