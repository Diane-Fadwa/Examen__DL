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

asset_rattrapage = Asset("replay://rattrapage")


def to_hdfs_machine_path(hdfs_path: str) -> str:
    if hdfs_path.startswith("hdfs://"):
        return "/" + hdfs_path.split("/", 3)[3]
    return hdfs_path


@task
def validate_rattrapage_payload():
    ctx = get_current_context()
    events = ctx["triggering_asset_events"][asset_rattrapage]
    payload = events[-1].extra

    return [
        {
            "contract_path": payload["contract_path"],
            "file_path": f,
        }
        for f in payload["files"]
    ]


with DAG(
    dag_id="dag_rattrapage",
    start_date=datetime(2026, 1, 1),
    schedule=[asset_rattrapage],
    catchup=False,
    tags=["rattrapage"],
) as dag:

    files = validate_rattrapage_payload()

    @task_group
    def process_file(contract_path: str, file_path: str):

        @task
        def decompress_if_needed(file_path: str) -> str:
            if not file_path.endswith(".zstd"):
                return file_path

            ssh_hook = SSHHook(ssh_conn_id="SSH_REC")

            input_path = to_hdfs_machine_path(file_path)
            output_path = input_path.replace(".zstd", "")

            logger.info("HCOMPRESSOR INPUT  = %s", input_path)
            logger.info("HCOMPRESSOR OUTPUT = %s", output_path)

            cmd = f"""
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
            ./hcompressor \
              --mode decompression \
              --compression_type zstd \
              --delete_input 0 \
              {input_path} {output_path}
            """

            exit_code, stdout, stderr = ssh_hook.exec_ssh_client_command(
                ssh_client=ssh_hook.get_conn(),
                command=cmd,
                get_pty=True,
            )

            if exit_code != 0:
                raise RuntimeError(
                    f"hcompressor failed\nstdout={stdout}\nstderr={stderr}"
                )

            return "hdfs://nameservice1" + output_path

        @task
        def spark_ingest(contract_path: str, file_path: str):
            livy = KnoxLivyHook(conn_id="KNOX_REC")

            job_name = f"rattrapage_{Path(file_path).stem}_{random.randint(1000,9999)}"

            livy.post_batch(
                file="hdfs://nameservice1/awb_rec/awb_ingestion/artifacts/"
                     "ebk_web_device_history/check_meta_from_contract.py",
                name=job_name,
                args=[contract_path, file_path],
                driver_memory="1g",
                executor_memory="2g",
                num_executors=2,
            )

        decompressed = decompress_if_needed(file_path)
        spark_ingest(contract_path, decompressed)

    process_file.expand_kwargs(files)
