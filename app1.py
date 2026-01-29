[2026-01-29, 12:45:23] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-29, 12:45:23] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-29, 12:45:24] WARNING - /home/airflow/.local/lib/python3.12/site-packages/airflow/models/connection.py:471: DeprecationWarning: Using Connection.get_connection_from_secrets from `airflow.models` is deprecated.Please use `get` on Connection from sdk(`airflow.sdk.Connection`) instead
  warnings.warn(
: source="py.warnings"
[2026-01-29, 12:45:24] INFO - Connection Retrieved 'SSH_REC': source="airflow.hooks.base"
[2026-01-29, 12:45:24] WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:24] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-29, 12:45:24] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-29, 12:45:24] INFO - Authentication (password) successful!: source="paramiko.transport"
[2026-01-29, 12:45:24] INFO - Décompression via hcompressor : /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-29, 12:45:24] INFO - Running command: 
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin &&             ./hcompressor               --mode decompression               --compression_type zstd               --delete_input 0               /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502
            : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:24] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:24] INFO - 3919542 | 2026-01-29 12:45:24,996 | hcompressor | INFO | Arguments parsed successfully: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:45] INFO - 3919542 | 2026-01-29 12:45:45,187 | hcompressor | INFO | 1 files to uncompress: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:45] INFO - 3919542 | 2026-01-29 12:45:45,187 | hcompressor | INFO | Decompressing /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd/ebk_web_device_history_20250502.txt.zstd to /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502/ebk_web_device_history_20250502.txt: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:54] INFO - 3919542 | 2026-01-29 12:45:54,411 | hcompressor | INFO | Time taken for zstd decompression: 9.223544 seconds: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:54] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:45:54] INFO - Fichier décompressé prêt pour Spark : hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-29, 12:45:54] INFO - Done. Returned value was: hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502: source="airflow.task.operators.airflow.providers.standard.decorators.python._PythonDecoratedOperator"
[2026-01-29, 12:45:54] INFO - Pushing xcom: ti="RuntimeTaskInstance(id=UUID('019c0992-58bf-757d-9432-c251933ce88e'), task_id='process_file.decompress_if_needed', dag_id='dag_rattrapage', run_id='asset_triggered__2026-01-29T11:45:11.140845+00:00_PGCRss6v', try_number=1, map_index=0, hostname='airflow3-worker-0.airflow3-worker.dev-llm-lab.svc.cluster.local', context_carrier={}, task=<Task(_PythonDecoratedOperator): process_file.decompress_if_needed>, bundle_instance=LocalDagBundle(name=dags-folder), max_tries=1, start_date=datetime.datetime(2026, 1, 29, 11, 45, 20, 86848, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, log_url='http://localhost:8080/dags/dag_rattrapage/runs/asset_triggered__2026-01-29T11%3A45%3A11.140845%2B00%3A00_PGCRss6v/tasks/process_file.decompress_if_needed/mapped/0?try_number=1')": source="task"


[2026-01-29, 12:53:12] ERROR - Batch 1281 failed with state: dead: source="airflow.task.hooks.awb_lib.providers.knox.hooks.knox_livy_hook.KnoxLivyHook"
[2026-01-29, 12:53:12] ERROR - Task failed with exception: source="task"
AirflowException: Batch 1281 failed with state: dead
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 172 in spark_validate_ingest

File "/opt/airflow/dags/repo/awb_lib/providers/knox/hooks/knox_livy_hook.py", line 400 in poll_for_completion


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

            # CHEMIN pour hcompressor → enlever hdfs://nameservice1
            if file_path.startswith("hdfs://nameservice1"):
                compressor_input_path = file_path.replace("hdfs://nameservice1", "", 1)
            else:
                compressor_input_path = file_path

            compressor_output_path = compressor_input_path.removesuffix(".zstd")

            cmd = f"""
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
            ./hcompressor \
              --mode decompression \
              --compression_type zstd \
              --delete_input 0 \
              {compressor_input_path} {compressor_output_path}
            """

            logger.info("Décompression via hcompressor : %s", compressor_input_path)

            exit_code, stdout, stderr = ssh_hook.exec_ssh_client_command(
                ssh_client=ssh_client,
                command=cmd,
                get_pty=True,
                environment=None,
                timeout=600
            )

            if exit_code != 0:
                raise RuntimeError(
                    f"Erreur hcompressor (exit={exit_code})\n"
                    f"stdout={stdout.decode()}\n"
                    f"stderr={stderr.decode()}"
                )

            # Reconstruction du path HDFS pour Spark
            hdfs_output_path = "hdfs://nameservice1" + compressor_output_path
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

