import logging
from airflow.sdk import Asset, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# Asset de rattrapage 

asset_rattrapage = Asset("replay://rattrapage")

# Validation du payload porté par l’Asset (publication)

@task
def validate_rattrapage_payload():
    """
    Récupère le JSON depuis dag_run.conf (trigger manuel)
    et sécurise le payload AVANT publication de l’Asset.

    Format attendu :
    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }

     Validation volontairement légère ici :
    - Sécurité structurelle
    - Pas de validation métier lourde
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

    # Vérifications simples
    if not isinstance(payload["contract_path"], str):
        raise ValueError("'contract_path' must be a string")

    if not isinstance(payload["files"], list) or not payload["files"]:
        raise ValueError("'files' must be a non-empty list")

    logger.info(
        "Asset rattrapage validé pour publication : contract=%s | %d fichiers",
        payload["contract_path"],
        len(payload["files"]),
    )

    return payload
