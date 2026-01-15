import logging
import random
from datetime import datetime
from pathlib import Path
from airflow.datasets import Dataset

from airflow.sdk import DAG, task, task_group
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
        raise ValueError("No triggering asset event found for replay://rattrapage")

    asset_event = events[asset_rattrapage][0]
    payload = asset_event.extra

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")
        
    # contract_path
    
    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in Asset JSON")

    contract_path = payload["contract_path"]

    if not isinstance(contract_path, str):
        raise ValueError("'contract_path' must be a string")

    if not contract_path.startswith("/"):
        raise ValueError("'contract_path' must be an absolute path")

    if not contract_path.startswith("/contracts/"):
        raise ValueError("'contract_path' must be under /contracts/")

    if not contract_path.endswith((".yml", ".yaml")):
        raise ValueError("'contract_path' must be a YAML file")

    # files
    
    if "files" not in payload:
        raise ValueError("Missing 'files' in Asset JSON")

    files = payload["files"]

    if not isinstance(files, list):
        raise ValueError("'files' must be a list")

    if len(files) == 0:
        raise ValueError("'files' list is empty")

    for f in files:
        if not isinstance(f, str):
            raise ValueError("Each file must be a string")
        if not f.startswith("/"):
            raise ValueError(f"File path must be absolute: {f}")
        if not f.startswith("/raw/"):
            raise ValueError(f"File must be under /raw/: {f}")

    logger.info(
        "Rattrapage Asset validated: contract=%s, %d files",
        contract_path,
        len(files),
    )

    return {
        "contract_path": contract_path,
        "files": files,
    }


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
