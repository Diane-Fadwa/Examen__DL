@task
def decompress_if_needed(file_path: str) -> str:
    if not file_path.endswith(".zstd"):
        logger.info("Fichier non compressé : %s", file_path)
        return file_path

    ssh_hook = SSHHook(ssh_conn_id="SSH_REC")
    ssh_client = ssh_hook.get_conn()

    # PAS d'enlèvement de hdfs://nameservice1
    machine_input_path = file_path
    machine_output_path = file_path.removesuffix(".zstd")  # sortie en HDFS

    cmd = f"""
    cd /opt/workspace/Script/CEKO/hcomp/lib/bin && \
    ./hcompressor \
      --mode decompression \
      --compression_type zstd \
      --delete_input 0 \
      {machine_input_path} {machine_output_path}
    """

    logger.info("Décompression via SSH : %s", machine_input_path)

    exit_code, stdout, stderr = ssh_hook.exec_ssh_client_command(
        ssh_client=ssh_client,
        command=cmd,
        get_pty=True,
        environment=None,
    )

    if exit_code != 0:
        raise RuntimeError(
            f"Erreur hcompressor (exit={exit_code})\n"
            f"stdout={stdout.decode()}\n"
            f"stderr={stderr.decode()}"
        )

    # Retourne chemin HDFS complet pour Spark
    hdfs_output_path = machine_output_path
    logger.info("Fichier décompressé prêt pour Spark : %s", hdfs_output_path)
    return hdfs_output_path
