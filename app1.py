[2026-01-29, 11:38:34] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-29, 11:38:34] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-29, 11:38:35] WARNING - /home/airflow/.local/lib/python3.12/site-packages/airflow/models/connection.py:471: DeprecationWarning: Using Connection.get_connection_from_secrets from `airflow.models` is deprecated.Please use `get` on Connection from sdk(`airflow.sdk.Connection`) instead
  warnings.warn(
: source="py.warnings"
[2026-01-29, 11:38:35] INFO - Connection Retrieved 'SSH_REC': source="airflow.hooks.base"
[2026-01-29, 11:38:35] WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:35] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-29, 11:38:35] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-29, 11:38:35] INFO - Authentication (password) successful!: source="paramiko.transport"
[2026-01-29, 11:38:35] INFO - Décompression via hcompressor : /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-29, 11:38:35] INFO - Running command: 
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin &&             ./hcompressor               --mode decompression               --compression_type zstd               --delete_input 0               /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502
            : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:35] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:35] INFO - 3872477 | 2026-01-29 11:38:35,693 | hcompressor | INFO | Arguments parsed successfully: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO - Traceback (most recent call last):: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -   File "./hcompressor", line 22, in <module>: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -     hcompressor.hcompressor.main(): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 72, in main: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -     dfs.validate_hdfs_path(input_path): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py", line 61, in validate_hdfs_path: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO -     raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path "): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] INFO - hcompressor.hdfs_utils.InvalidHdfsPathException: /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd is not a valid hdfs path : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 11:38:39] ERROR - Task failed with exception: source="task"
RuntimeError: Erreur hcompressor (exit=1)
stdout=
3872477 | 2026-01-29 11:38:35,693 | hcompressor | INFO | Arguments parsed successfully
Traceback (most recent call last):
  File "./hcompressor", line 22, in <module>
    hcompressor.hcompressor.main()
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 72, in main
    dfs.validate_hdfs_path(input_path)
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py", line 61, in validate_hdfs_path
    raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path ")
hcompressor.hdfs_utils.InvalidHdfsPathException: /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd is not a valid hdfs path 

stderr=
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 125 in decompress_if_needed

[2026-01-29, 11:38:46] ERROR - Top level error: source="task"
AirflowRuntimeError: API_SERVER_ERROR: {'status_code': 404, 'message': 'Server returned error', 'detail': {'detail': {'reason': 'not_found', 'message': 'Task Instance not found'}}}
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1351 in main

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 999 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 204 in send

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 264 in _get_response

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 251 in _from_frame

[2026-01-29, 11:38:46] WARNING - Process exited abnormally: exit_code=1: source="task
