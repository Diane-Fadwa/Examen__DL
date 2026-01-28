import logging
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sdk import Asset
from airflow.operators.python import get_current_context
from airflow.providers.ssh.hooks.ssh import SSHHook

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook

logger = logging.getLogger(__name__)

# Asset consommé
asset_rattrapage = Asset("replay://rattrapage")


# ============================================================
# Validation + explosion du JSON porté par l’AssetEvent
# ============================================================

@task
def validate_rattrapage_payload():
    ctx = get_current_context()

    events = ctx.get("triggering_asset_events")
    if not events:
        raise ValueError("No triggering asset events found")

    asset_events = events.get(asset_rattrapage)
    if not asset_events:
        raise ValueError("No events found for asset replay://rattrapage")

    payload = asset_events[-1].extra
    logger.info("Payload reçu depuis l'Asset : %s", payload)

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    if "contract_path" not in payload or "files" not in payload:
        raise ValueError("Missing required fields")

    contract_path = payload["contract_path"]
    files = payload["files"]

    if not contract_path.startswith("hdfs://"):
        raise ValueError("'contract_path' must be an HDFS path")

    for f in files:
        if not f.startswith("hdfs://"):
            raise ValueError(f"Invalid HDFS path: {f}")

    return [
        {
            "contract_path": contract_path,
            "file_path": f,
        }
        for f in files
    ]


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par Asset",
    start_date=datetime(2026, 1, 1),
    schedule=[asset_rattrapage],
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["rattrapage", "asset"],
) as dag:

    files_to_process = validate_rattrapage_payload()

    # ============================================================
    # Traitement par fichier
    # ============================================================

    @task_group
    def process_file(contract_path: str, file_path: str):

        # ----------------------------
        # Décompression si nécessaire
        # ----------------------------
        @task
        def decompress_if_needed(file_path: str) -> str:
            if not file_path.endswith(".zstd"):
                logger.info("Fichier non compressé, passage direct : %s", file_path)
                return file_path

            ssh_hook = SSHHook(ssh_conn_id="SSH_REC_RATTRAPAGE")

            input_path = file_path
            output_path = file_path.replace(".zstd", "")

            cmd = f"""
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
            ./hcompressor \
              --mode decompression \
              --compression_type zstd \
              --delete_input 0 \
              {input_path} {output_path}
            """

            logger.info("Décompression du fichier via SSH : %s", input_path)
            ssh_hook.run_command(command=cmd)

            logger.info("Fichier décompressé : %s", output_path)
            return output_path

        # ----------------------------
        # Spark job
        # ----------------------------
        @task
        def spark_validate_ingest(contract_path: str, file_path: str):
            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = random.randint(1000, 9999)

            job_name = f"rattrapage_{Path(file_path).stem}_{timestamp}_{random_suffix}"

            logger.info("Submitting Spark job %s", job_name)
            logger.info("Input file: %s", file_path)

            batch_id = livy_hook.post_batch(
                file="hdfs://nameservice1/awb_rec/awb_ingestion/artifacts/"
                     "ebk_web_device_history/check_meta_from_contract.py",
                name=job_name,
                args=[contract_path, file_path],
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

            logger.info("Spark job terminé avec l’état %s", final_state.value)

        decompressed_file = decompress_if_needed(file_path)
        spark_validate_ingest(contract_path, decompressed_file)

    process_file.expand_kwargs(files_to_process)
