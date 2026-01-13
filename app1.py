import logging
import random
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, Asset, task, task_group
from airflow.operators.python import get_current_context
from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from config import NAMENODE

logger = logging.getLogger(__name__)

# Asset de rattrapage (replay à la demande)

asset_rattrapage = Asset("replay://rattrapage")

# Validation du JSON porté par l’Asset

@task
def validate_rattrapage_payload():
    """
    Valide le JSON envoyé avec l'Asset via le contexte Airflow.
    """
    ctx = get_current_context()
    events = ctx.get("triggering_asset_events", {})

    if not events or asset_rattrapage not in events:
        raise ValueError("No triggering asset event found for rattrapage")

    # On prend le premier événement pour cet asset
    asset_event = events[asset_rattrapage][0]
    payload = asset_event.extra

    if not payload:
        raise ValueError("Asset payload is empty")

    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in Asset JSON")

    if "files" not in payload:
        raise ValueError("Missing 'files' in Asset JSON")

    if not isinstance(payload["files"], list):
        raise ValueError("'files' must be a list")

    if len(payload["files"]) == 0:
        raise ValueError("'files' list is empty")

    logger.info(
        "Rattrapage Asset validated: contract=%s, %d files",
        payload["contract_path"],
        len(payload["files"]),
    )

    return payload

# DAG déclenché uniquement par l’Asset

with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par Asset replay://rattrapage",
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 1},
    schedule=[asset_rattrapage],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rattrapage", "replay"],
) as dag:

    # 1. Validation du JSON de l’Asset
    payload = validate_rattrapage_payload()

    # 2. Explosion des fichiers pour mapping dynamique
    @task
    def explode_files(payload):
        return [
            {"contract_path": payload["contract_path"], "file_path": f}
            for f in payload["files"]
        ]

    files_to_process = explode_files(payload)

    # 3. Traitement d’un fichier (1 Spark job par fichier)
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

            final_state = livy_hook.poll_for_completion(
                session_id=batch_id, polling_interval=30, max_polling_attempts=120
            )

            logger.info(
                "Spark job %s finished with state %s", batch_id, final_state.value
            )

            return {"batch_id": batch_id, "file": file_path, "state": final_state.value}

        spark_validate_ingest(contract_path, file_path)

    # 4. Mapping dynamique : 1 job Spark par fichier
    process_file.expand_kwargs(files_to_process)





[2026-01-13, 11:41:32] ERROR - Task failed with exception: source="task"
ValueError: 'files' must be a list
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 44 in validate_rattrapage_payload
