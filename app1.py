import logging
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sdk import Asset, get_current_context
from airflow.providers.ssh.hooks.ssh import SSHHook

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook

logger = logging.getLogger(__name__)

# Asset consommé
asset_rattrapage = Asset("replay://rattrapage")

# Validation + explosion du JSON porté par l’AssetEvent
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

    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path'")

    if "files" not in payload:
        raise ValueError("Missing 'files'")

    contract_path = payload["contract_path"]
    files = payload["files"]

    if not isinstance(contract_path, str) or not contract_path.startswith("hdfs://"):
        raise ValueError("'contract_path' must be an HDFS path")

    if not isinstance(files, list) or not files:
        raise ValueError("'files' must be a non-empty list")

    for f in files:
        if not isinstance(f, str) or not f.startswith("hdfs://"):
            raise ValueError(f"Invalid HDFS file path: {f}")

    logger.info(
        "Rattrapage validé : contract=%s | %d fichiers",
        contract_path,
        len(files),
    )

    # Explosion : 1 fichier = 1 exécution
    return [
        {
            "contract_path": contract_path,
            "file_path": f,
        }
        for f in files
    ]


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

    files_to_process = validate_rattrapage_payload()

    # Traitement par fichier
    @task_group
    def process_file(contract_path: str, file_path: str):

        # Décompression si nécessaire (.zstd uniquement)
        @task
        def decompress_if_needed(file_path: str) -> str:
            if not file_path.endswith(".zstd"):
                logger.info("Fichier non compressé : %s", file_path)
                return file_path

            ssh_hook = SSHHook(ssh_conn_id="SSH_REC")
            ssh_client = ssh_hook.get_conn()

            # CHEMIN pour hcompressor → enlever hdfs://nameservice1 ou triple slash
            if file_path.startswith("hdfs://nameservice1"):
                machine_input_path = file_path.replace("hdfs://nameservice1", "", 1)
            elif file_path.startswith("hdfs:///"):
                machine_input_path = file_path.replace("hdfs://", "", 1)
            else:
                machine_input_path = file_path

            machine_output_path = machine_input_path.removesuffix(".zstd")

            cmd = f"""
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
            ./hcompressor \
              --mode decompression \
              --compression_type zstd \
              --delete_input 0 \
              {machine_input_path} {machine_output_path}
            """

            logger.info("Décompression via hcompressor : %s", file_path)

            exit_code, stdout, stderr = ssh_hook.exec_ssh_client_command(
                ssh_client=ssh_client,
                command=cmd,
                get_pty=True,
                environment=None,
            )

            if exit_code != 0:
                raise RuntimeError(
                    f"Erreur hcompressor (exit={exit_code})\n"
                    f"stdout={stdout.decode()}\n"
                    f"stderr={stderr.decode()}"
                )

            # Reconstruction du path HDFS pour Spark
            hdfs_output_path = "hdfs://nameservice1" + machine_output_path
            logger.info("Fichier décompressé prêt pour Spark : %s", hdfs_output_path)
            return hdfs_output_path

        # Spark validation + ingestion
        @task
        def spark_validate_ingest(contract_path: str, file_path: str):
            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = random.randint(1000, 9999)

            job_name = f"rattrapage_{Path(file_path).stem}_{timestamp}_{random_suffix}"

            logger.info("Submitting Spark job %s", job_name)
            logger.info("Contract: %s", contract_path)
            logger.info("Input file: %s", file_path)

            batch_id = livy_hook.post_batch(
                file="hdfs://nameservice1/awb_rec/awb_ingestion/artifacts/"
                     "ebk_web_device_history/check_meta_from_contract.py",
                name=job_name,
                args=[
                    contract_path,
                    file_path,
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

        # Chaînage logique
        decompressed_file = decompress_if_needed(file_path)
        spark_validate_ingest(contract_path, decompressed_file)

    # Mapping dynamique
    process_file.expand_kwargs(files_to_process)
