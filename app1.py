import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)

#  payload JSON pour le rattrapage

def get_rattrapage_payload():
    """
    Retourne le JSON attendu pour le rattrapage.
    Exemple de format :
    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }
    """
    payload = {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ],
    }

    return payload


# Validation du JSON

@task
def validate_rattrapage_payload(payload: dict):
    """
    Valide que le JSON reçu respecte le template attendu :

    {
        "contract_path": "/contracts/client.yml",
        "files": [
            "/raw/file1.txt",
            "/raw/file2.txt"
        ]
    }

    :param payload: JSON à valider
    :return: payload validé
    """

    if not payload:
        raise ValueError("Payload is empty")

    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")

    if "contract_path" not in payload:
        raise ValueError("Missing 'contract_path' in payload")

    if "files" not in payload:
        raise ValueError("Missing 'files' in payload")

    if not isinstance(payload["files"], list):
        raise ValueError("'files' must be a list in payload")

    if len(payload["files"]) == 0:
        raise ValueError("'files' list is empty")

    logger.info(
        "Rattrapage payload validated: contract=%s, %d files",
        payload["contract_path"],
        len(payload["files"]),
    )

    return payload
