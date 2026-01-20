import logging
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sdk import Asset
from airflow.operators.python import get_current_context

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from config import NAMENODE

logger = logging.getLogger(__name__)

# Asset consommé 

asset_rattrapage = Asset("replay://rattrapage")


#  Validation du JSON porté par l’AssetEvent

@task
def validate_rattrapage_payload():
    """
    Récupère et valide le JSON depuis AssetEvent.extra
    """

    ctx = get_current_context()

    events = ctx.get("triggering_asset_events")

    if not events:
        raise ValueError("No triggering asset events found")

    # Récupération du DERNIER event publié pour replay://rattrapage
    asset_events = events.get(asset_rattrapage)

    if not asset_events:
        raise ValueError("No events found for asset replay://rattrapage")

    payload = asset_events[-1].extra

    logger.info("Payload reçu depuis l'Asset : %s", payload)

    
    #  VALIDATION MÉTIER 
    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    # contract_path
    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path'")

    contract_path = payload["contract_path"]

    if not isinstance(contract_path, str):
        raise ValueError("'contract_path' must be a string")

    if not contract_path.startswith("/contracts/"):
        raise ValueError("'contract_path' must be under /contracts/")

    if not contract_path.endswith((".yml", ".yaml")):
        raise ValueError("'contract_path' must be a YAML file")

    # files
    if "files" not in payload:
        raise ValueError("Missing 'files'")

    files = payload["files"]

    if not isinstance(files, list) or not files:
        raise ValueError("'files' must be a non-empty list")

    for f in files:
        if not isinstance(f, str):
            raise ValueError("Each file must be a string")
        if not f.startswith("/raw/"):
            raise ValueError(f"File must be under /raw/: {f}")

    logger.info(
        "Rattrapage validé : contract=%s | %d fichiers",
        contract_path,
        len(files),
    )

    return {
        "contract_path": contract_path,
        "files": files,
    }


# DAG déclenché UNIQUEMENT par l’Asset

with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par l’Asset replay://rattrapage",
    start_date=datetime(2026, 1, 1),
    schedule=[asset_rattrapage],  
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["rattrapage", "asset", "replay"],
) as dag:

    # Validation du payload Asset

    payload = validate_rattrapage_payload()

    #  Explosion des fichiers (Dynamic Task Mapping)

    @task
    def explode_files(payload: dict):
        """
        Transforme :
        {
            contract_path: "...",
            files: [f1, f2, f3]
        }
        en :
        [
            {contract_path, file_path=f1},
            {contract_path, file_path=f2},
            ...
        ]
        """
        return [
            {
                "contract_path": payload["contract_path"],
                "file_path": f,
            }
            for f in payload["files"]
        ]

    files_to_process = explode_files(payload)

    # Traitement d’un fichier (1 Spark job par fichier)

    @task_group
    def process_file(contract_path: str, file_path: str):

        @task
        def spark_validate_ingest(contract_path: str, file_path: str):
            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = random.randint(1000, 9999)

            job_name = (
                f"rattrapage_{Path(file_path).stem}_{timestamp}_{random_suffix}"
            )

            logger.info("Submitting Spark job %s", job_name)
            logger.info("Contract: %s", contract_path)
            logger.info("Input file: %s", file_path)

            batch_id = livy_hook.post_batch(
                file=f"{NAMENODE}/scripts/check_meta_from_contract.py",
                name=job_name,
                args=[
                    f"{NAMENODE}{contract_path}",
                    f"{NAMENODE}{file_path}",
                ],
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
                session_id=batch_id,
                polling_interval=30,
                max_polling_attempts=120,
            )

            logger.info(
                "Spark job %s terminé avec l’état %s",
                batch_id,
                final_state.value,
            )

            return {
                "batch_id": batch_id,
                "file": file_path,
                "state": final_state.value,
            }

        spark_validate_ingest(contract_path, file_path)

    # Mapping dynamique : 1 job Spark par fichier

    process_file.expand_kwargs(files_to_process)
