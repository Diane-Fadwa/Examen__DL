@task
def validate_rattrapage_payload():
    """
    Récupère et valide le JSON depuis AssetEvent.extra
    """

    ctx = get_current_context()

    events = ctx.get("triggering_asset_events")

    if not events:
        raise ValueError("No triggering asset events found")

    # 🔹 Récupération du DERNIER event publié pour replay://rattrapage
    asset_events = events.get(asset_rattrapage)

    if not asset_events:
        raise ValueError("No events found for asset replay://rattrapage")

    payload = asset_events[-1].extra

    logger.info("Payload reçu depuis l'Asset : %s", payload)

    # ============================
    # 🔒 VALIDATION MÉTIER (INCHANGÉE)
    # ============================

    if not isinstance(payload, dict):
        raise ValueError("Asset payload must be a JSON object")

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
