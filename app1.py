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

