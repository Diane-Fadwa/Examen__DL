import logging
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# Dataset de rattrapage (ex-Asset)
asset_rattrapage = Dataset("replay://rattrapage")


@task
def validate_rattrapage_payload():
    """
    Valide le JSON porté par le DatasetEvent.
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

    if "dataset_events" not in ctx or not ctx["dataset_events"]:
        raise ValueError("No DatasetEvent found")

    dataset_event = ctx["dataset_events"][0]
    payload = dataset_event.extra

    if not isinstance(payload, dict):
        raise ValueError("Dataset payload must be a JSON object")

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

    if not isinstance(files, list):
        raise ValueError("'files' must be a list")

    if len(files) == 0:
        raise ValueError("'files' list is empty")

    for f in files:
        if not isinstance(f, str):
            raise ValueError("Each file must be a string")
        if not f.startswith("/raw/"):
            raise ValueError(f"File must be under /raw/: {f}")

    logger.info(
        "Rattrapage dataset validated: contract=%s, %d files",
        contract_path,
        len(files),
    )

    return {
        "contract_path": contract_path,
        "files": files,
    }
