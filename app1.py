import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import random

from airflow.sdk import DAG, Asset, task, task_group

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from awb_lib.providers.knox.hooks.knox_webhdfs_hook import KnoxWebHDFSHook
from config import NAMENODE, PUT_HDFS_POOL, OCP_DAGS_FOLDER_PREFIX

from dags.ingestion.dag_factory_utils import upload_file_to_hdfs

logger = logging.getLogger(__name__)

# Asset de rattrapage

asset_rattrapage = Asset("replay://rattrapage")

# Validation JSON de l'Asset

@task
def validate_rattrapage_payload(asset_event):
    """
    Assure que l'Asset JSON suit le template attendu:
    {
        "contract_path": "path/contrat.yml",
        "files": ["/raw/file1.txt", "/raw/file2.txt", ...]
    }
    """
    payload = asset_event.extra
    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in Asset JSON")
    if "files" not in payload or not isinstance(payload["files"], list):
        raise ValueError("'files' must be a list in Asset JSON")
    if len(payload["files"]) == 0:
        raise ValueError("'files' list is empty")
    return payload

# DAG de Ratt

with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage à la demande via Asset replay://rattrapage",
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 1},
    schedule=[asset_rattrapage],
    start_date=datetime.strptime("2026-01-12", "%Y-%m-%d"),
    catchup=False,
    tags="RATT",
) as dag:

    #  Valider le payload
    payload = validate_rattrapage_payload(asset_rattrapage)

    #  Préparer les fichiers (pour la boucle)
    @task
    def explode_files(payload):
        return [{"contract_path": payload["contract_path"], "file_path": f} for f in payload["files"]]

    files_to_process = explode_files(payload)

    # pour chaque fichier

    @task_group
    def process_file(contract_path: str, file_path: str):
        """
        Chaque fichier passe par upload HDFS + check/ingestion via Livy.
        """

        # Construire paths HDFS
        artifacts_dir = f"/tmp/rattrapage/artifacts/{Path(file_path).stem}"
        contract_hdfs_path = f"{artifacts_dir}/{Path(contract_path).name}"
        pyspark_script_local = f"{OCP_DAGS_FOLDER_PREFIX}/scripts/check_meta_from_contract.py"
        pyspark_script_hdfs_path = f"{artifacts_dir}/check_meta_from_contract.py"

        #  Upload contract + script PySpark sur HDFS
        @task
        def upload_artifacts():
            webhdfs_hook = KnoxWebHDFSHook(conn_id="KNOX_REC")
            logger.info(f"Creating artifacts directory: {artifacts_dir}")
            webhdfs_hook.create_directory(artifacts_dir, overwrite=True)

            logger.info(f"Uploading contract: {contract_path} -> {contract_hdfs_path}")
            webhdfs_hook.upload_file(contract_path, contract_hdfs_path, overwrite=True)

            logger.info(f"Uploading PySpark script: {pyspark_script_local} -> {pyspark_script_hdfs_path}")
            webhdfs_hook.upload_file(pyspark_script_local, pyspark_script_hdfs_path, overwrite=True)

            return {"contract_hdfs_path": contract_hdfs_path, "script_hdfs_path": pyspark_script_hdfs_path}

        # Spark validation + ingestion via Livy
        @task
        def spark_ingest(artifact_paths, upload_result):
            hdfs_file_path = upload_result.get("hdfs_file_path", file_path)
            livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

            job_name = f"rattrapage_{Path(file_path).stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{random.randint(1000,9999)}"

            logger.info(f"Submitting Spark job {job_name} for file {hdfs_file_path}")

            batch_id = livy_hook.post_batch(
                file=f"{NAMENODE}{artifact_paths['script_hdfs_path']}",
                name=job_name,
                args=[f"{NAMENODE}{artifact_paths['contract_hdfs_path']}", f"{hdfs_file_path}"],
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
            logger.info(f"Spark job {batch_id} completed: {final_state.value}")
            return {"batch_id": batch_id, "file": hdfs_file_path, "state": final_state.value}

        artifact_paths = upload_artifacts()
        file_uploaded = upload_file(file_path)
        spark_ingest(artifact_paths, file_uploaded)

    # Mapping dynamique sur tous les fichiers
    process_results = process_file.expand_kwargs(files_to_process)

    # Cleanup artefacts HDFS 
    @task
    def cleanup_artifacts():
        webhdfs_hook = KnoxWebHDFSHook(conn_id="KNOX_REC")
        cleanup_path = "/tmp/rattrapage/artifacts"
        logger.info(f"Cleaning up HDFS artifacts directory: {cleanup_path}")
        webhdfs_hook.delete_path(cleanup_path, recursive=True)
        return {"status": "cleaned"}

    cleanup = cleanup_artifacts()
    process_results >> cleanup



 @task
            def spark_validate_ingest(upload_result, file_path, raw_dir):
                """Execute PySpark validation and ingestion via Livy."""
                import os
                from datetime import datetime

                # Use the workflow_name from the closure (contract-level)
                wf_name = workflow_name

                # Build HDFS file path from raw_dir and file_path
                filename = os.path.basename(file_path)

                # Check if upload_result contains the path
                if "hdfs_file_path" in upload_result:
                    hdfs_file_path = upload_result["hdfs_file_path"]
                elif "output_file" in upload_result:
                    hdfs_file_path = upload_result["output_file"]
                else:
                    # Fallback: construct from raw_dir
                    hdfs_file_path = f"{raw_dir.rstrip('/')}/{filename}"

                logger.info(f"Using HDFS file path: {hdfs_file_path}")

                # Initialize Livy hook
                livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

                # Create unique job name using timestamp and filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:19]
                # Add a random component for additional uniqueness
                import random

                random_suffix = random.randint(1000, 9999)
                job_name = f"contract_ingestion_{wf_name}_{timestamp}_{random_suffix}"

                logger.info(f"Submitting Spark job: {job_name}")
                logger.info(f"Workflow: {wf_name}")
                logger.info(f"Contract path: {contract_hdfs_path}")
                logger.info(f"Input file: {hdfs_file_path}")

                # Submit batch job
                batch_id = livy_hook.post_batch(
                    file=f"{namenode}{pyspark_script_hdfs_path}",
                    name=job_name,
                    args=[
                        f"{namenode}{contract_hdfs_path}",
                        f"{hdfs_file_path}",
                    ],
                    queue=queue,
                    conf={
                        "spark.sql.sources.partitionOverwriteMode": "dynamic",
                        "spark.sql.adaptive.enabled": "true",
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": "2",
                        "spark.dynamicAllocation.maxExecutors": "3",
                    },
                    driver_memory="1g",
                    driver_cores=1,
                    executor_memory="2g",
                    executor_cores=2,
                    num_executors=2,
                )

                logger.info(f"Batch submitted with ID: {batch_id}")

                # Poll for completion
                final_state = livy_hook.poll_for_completion(
                    session_id=batch_id,
                    polling_interval=30,
                    max_polling_attempts=120,
                )

                logger.info(f"Spark job completed with state: {final_state}")

                return {
                    "batch_id": batch_id,
                    "final_state": final_state.value,
                    "workflow_name": wf_name,
                    "job_name": job_name,
                }

            # Define task dependencies within the group
            upload_result = upload_task(file_path, raw_dir, archive_path)
            spark_validate_ingest(upload_result, file_path, raw_dir)
