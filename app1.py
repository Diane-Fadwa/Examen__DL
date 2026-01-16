/opt/airflow/dags/repo/rattrapage/asset_ratt.py
Timestamp: 2026-01-16, 09:37:01

Traceback (most recent call last):
  File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
  File "/opt/airflow/dags/repo/rattrapage/asset_ratt.py", line 6, in <module>
    from airflow.datasets import Asset
ImportError: cannot import name 'Asset' from 'airflow.datasets' (/home/airflow/.local/lib/python3.12/site-packages/airflow/datasets/__init__.py). Did you mean: 'assets'?


/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py
Timestamp: 2026-01-16, 09:37:01

Traceback (most recent call last):
  File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
  File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 8, in <module>
    from airflow.datasets import Asset
ImportError: cannot import name 'Asset' from 'airflow.datasets' (/home/airflow/.local/lib/python3.12/site-packages/airflow/datasets/__init__.py). Did you mean: 'assets'?



import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Asset
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# ============================================================
# Asset de rattrapage (DOIT matcher dag_rattrapage.py)
# ============================================================
asset_rattrapage = Asset("replay://rattrapage")


# ============================================================
# Validation minimale du payload avant publication de l’Asset
# ============================================================
@task
def validate_rattrapage_payload():
    """
    Récupère le JSON depuis dag_run.conf (trigger manuel)
    et vérifie qu’il respecte le contrat global.

    Format attendu :
    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }

    ⚠️ Validation volontairement plus légère que dans dag_rattrapage :
    - ici : on sécurise l’événement
    - là-bas : on applique la validation métier stricte
    """

    ctx = get_current_context()

    if "dag_run" not in ctx or not ctx["dag_run"].conf:
        raise ValueError("No payload provided in dag_run.conf")

    payload = ctx["dag_run"].conf

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    # Champs obligatoires
    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in payload")

    if "files" not in payload:
        raise ValueError("Missing 'files' in payload")

    # Vérifications simples (pas métier lourd ici)
    if not isinstance(payload["files"], list) or not payload["files"]:
        raise ValueError("'files' must be a non-empty list")

    if not isinstance(payload["contract_path"], str):
        raise ValueError("'contract_path' must be a string")

    logger.info(
        "Asset rattrapage prêt à être publié : contract=%s | %d fichiers",
        payload["contract_path"],
        len(payload["files"]),
    )

    return payload











# dag_rattrapage.py

import logging
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.datasets import Asset
from airflow.operators.python import get_current_context

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from config import NAMENODE

logger = logging.getLogger(__name__)

# ============================================================
# Asset consommé (DOIT matcher asset_rattrapage.py)
# ============================================================
asset_rattrapage = Asset("replay://rattrapage")


# ============================================================
# 1. Validation du JSON porté par l’AssetEvent
# ============================================================
@task
def validate_rattrapage_payload():
    """
    Récupère et valide le JSON depuis AssetEvent.extra

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

    if "asset_events" not in ctx or asset_rattrapage.uri not in ctx["asset_events"]:
        raise ValueError("Aucun AssetEvent trouvé pour replay://rattrapage")

    # Dernier event publié (cas où plusieurs replays)
    asset_event = ctx["asset_events"][asset_rattrapage.uri][-1]
    payload = asset_event.extra

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    # --------------------
    # contract_path
    # --------------------
    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path'")

    contract_path = payload["contract_path"]

    if not isinstance(contract_path, str):
        raise ValueError("'contract_path' must be a string")

    if not contract_path.startswith("/contracts/"):
        raise ValueError("'contract_path' must be under /contracts/")

    if not contract_path.endswith((".yml", ".yaml")):
        raise ValueError("'contract_path' must be a YAML file")

    # --------------------
    # files
    # --------------------
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


# ============================================================
# DAG déclenché UNIQUEMENT par l’Asset
# ============================================================
with DAG(
    dag_id="dag_rattrapage",
    description="DAG de rattrapage déclenché par l'Asset replay://rattrapage",
    start_date=datetime(2026, 1, 1),
    schedule=[asset_rattrapage],   # ⬅️ CLÉ
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["rattrapage", "asset"],
) as dag:

    # 1. Validation du JSON Asset
    payload = validate_rattrapage_payload()

    # ========================================================
    # 2. Explosion des fichiers (Dynamic Task Mapping)
    # ========================================================
    @task
    def explode_files(payload: dict):
        return [
            {
                "contract_path": payload["contract_path"],
                "file_path": f,
            }
            for f in payload["files"]
        ]

    files_to_process = explode_files(payload)

    # ========================================================
    # 3. Traitement d’un fichier (1 Spark job = 1 fichier)
    # ========================================================
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

    # 4. Mapping dynamique
    process_file.expand_kwargs(files_to_process)
