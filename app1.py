import logging
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# Asset de rattrapage

asset_rattrapage = Asset("replay://rattrapage")


# Validation du payload porté par l'Asset

@task
def validate_rattrapage_payload():
    """
    Récupère le JSON depuis l'AssetEvent.extra
    et valide le contrat métier.

    Format attendu :
    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }
    """

    context = get_current_context()

    if "asset_events" not in context or not context["asset_events"]:
        raise ValueError("No AssetEvent found in context")

    asset_event = context["asset_events"][0]
    payload = asset_event.extra

    if not payload:
        raise ValueError("Asset payload is empty")

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in Asset JSON")

    if "files" not in payload:
        raise ValueError("Missing 'files' in Asset JSON")

    if not isinstance(payload["files"], list):
        raise ValueError("'files' must be a list in Asset JSON")

    if len(payload["files"]) == 0:
        raise ValueError("'files' list is empty")

    # validation du path
    if not payload["contract_path"].startswith("/"):
        raise ValueError("contract_path must be an absolute path")

    logger.info(
        "Rattrapage Asset validated: contract=%s, %d files",
        payload["contract_path"],
        len(payload["files"]),
    )

    return payload
