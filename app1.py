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

# =========================
# Asset consommé
# =========================
asset_rattrapage = Asset("replay://rattrapage")


# =========================
# Validation du payload Asset
# =========================
@task
def validate_rattrapage_payload():
    ctx = get_current_context()
    events = ctx.get("triggering_asset_events")

    if not events or asset_rattrapage not in events:
        raise ValueError("Aucun AssetEvent pour replay://rattrapage")

    payload = events[asset_rattrapage][-1].extra
    logger.info("Payload Asset reçu : %s", payload)

    if not isinstance(payload, dict):
        raise ValueError("Le payload Asset doit être un objet JSON")

    contract_path = payload.get("contract_path")
    files = payload.get("files")

    if not contract_path or not contract_path.startswith("hdfs://"):
        raise ValueError("contract_path invalide")

    if not isinstance(files, list) or not files:
        raise ValueError("files doit être une liste non vide")

    for f in files:
        if not isinstance(f, str) or not f.startswith("hdfs://"):
            raise ValueError(f"Chemin HDFS invalide : {f}")

    logger.info(
        "Rattrapage validé : contract=%s | %d fichier(s)",
        contract_path,
        len(files),
    )

    return [
        {
            "contract_path": contract_path,
            "file_path": f,
        }
        for f in files
    ]


# =========================
# DAG
# =========================
with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par l’Asset replay://rattrapage",
    start_date=datetime(2026, 1, 1),
    schedule=[asset_rattrapage],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    tags=["rattrapage", "asset", "replay"],
) as dag:

    files_to_process = validate_rattrapage_payload()

    # =========================
    # Traitement par fichier
    # =========================
    @task_group
    def process_file(contract_path: str, file_path: str):

        # -------------------------
        # Décompression ZSTD
        # -------------------------
        @task
        def decompress_if_needed(file_path: str) -> str:
            if not file_path.endswith(".zstd"):
                logger.info("Fichier non compressé : %s", file_path)
                return file_path

            ssh_hook = SSHHook(ssh_conn_id="SSH_REC")
            ssh_client = ssh_hook.get_conn()

            # Suppression du préfixe HDFS
            local_input_path = file_path.replace(
                "hdfs://nameservice1", "", 1
            )

            # hcompressor crée un dossier de sortie
            local_output_dir = local_input_path.removesuffix(".zstd")

            logger.info(
                "Décompression hcompressor : %s -> %s",
                local_input_path,
                local_output_dir,
            )

            cmd = f"""
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
            ./hcompressor \
              --mode decompression \
              --compression_type zstd \
              --delete_input 0 \
              {local_input_path} {local_output_dir}
            """

            exit_code, stdout, stderr = ssh_hook.exec_ssh_client_command(
                ssh_client=ssh_client,
                command=cmd,
                get_pty=True,
                timeout=600,
                environment=None
            )

            if exit_code != 0:
                raise RuntimeError(
                    f"hcompressor failed (exit={exit_code})\n"
                    f"stdout={stdout.decode()}\n"
                    f"stderr={stderr.decode()}"
                )

            # Nom réel du fichier produit
            output_file_name = Path(local_input_path).stem
            hdfs_output_file = (
                "hdfs://nameservice1"
                + local_output_dir
                + f"/{output_file_name}"
            )

            logger.info(
                "Fichier décompressé prêt pour Spark : %s",
                hdfs_output_file,
            )

            return hdfs_output_file

        # -------------------------
        # Spark validation + ingestion
        # -------------------------
        @task
        def spark_validate_ingest(contract_path: str, file_path: str):
            if not file_path.endswith(".txt"):
                raise ValueError(f"Spark input invalide (pas un fichier) : {file_path}")

            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = random.randint(1000, 9999)

            job_name = (
                f"rattrapage_{Path(file_path).stem}_{timestamp}_{random_suffix}"
            )

            logger.info("Soumission Spark job : %s", job_name)
            logger.info("Contract : %s", contract_path)
            logger.info("Input file : %s", file_path)

            batch_id = livy_hook.post_batch(
                file=(
                    "hdfs://nameservice1/awb_rec/awb_ingestion/artifacts/"
                    "ebk_web_device_history/check_meta_from_contract.py"
                ),
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

            if final_state.value != "success":
                raise RuntimeError(
                    f"Spark job {batch_id} terminé avec l'état {final_state.value}"
                )

            logger.info(
                "Spark job %s terminé avec succès",
                batch_id,
            )

            return {
                "batch_id": batch_id,
                "file": file_path,
                "state": final_state.value,
            }

        # -------------------------
        # Chaînage
        # -------------------------
        decompressed_file = decompress_if_needed(file_path)
        spark_validate_ingest(contract_path, decompressed_file)

    # =========================
    # Mapping dynamique
    # =========================
    process_file.expand_kwargs(files_to_process)




[2026-01-30, 09:14:05] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-30, 09:14:05] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-30, 09:14:06] ERROR - Task failed with exception: source="task"
ValueError: Spark input invalide (pas un fichier) : hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502/ebk_web_device_history_20250502
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 161 in spark_validate_ingest
