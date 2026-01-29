[2026-01-29, 09:54:00] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-29, 09:54:00] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-29, 09:54:01] WARNING - /home/airflow/.local/lib/python3.12/site-packages/airflow/models/connection.py:471: DeprecationWarning: Using Connection.get_connection_from_secrets from `airflow.models` is deprecated.Please use `get` on Connection from sdk(`airflow.sdk.Connection`) instead
  warnings.warn(
: source="py.warnings"
[2026-01-29, 09:54:01] INFO - Connection Retrieved 'SSH_REC': source="airflow.hooks.base"
[2026-01-29, 09:54:01] WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:01] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-29, 09:54:01] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-29, 09:54:01] INFO - Authentication (password) successful!: source="paramiko.transport"
[2026-01-29, 09:54:01] INFO - Décompression via hcompressor : hdfs:///raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-29, 09:54:01] INFO - Running command: 
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin &&             ./hcompressor               --mode decompression               --compression_type zstd               --delete_input 0               hdfs:///raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd hdfs:///raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502
            : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:01] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:01] INFO - 3785434 | 2026-01-29 09:54:01,395 | hcompressor | INFO | Arguments parsed successfully: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO - Traceback (most recent call last):: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -   File "./hcompressor", line 22, in <module>: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -     hcompressor.hcompressor.main(): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 72, in main: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -     dfs.validate_hdfs_path(input_path): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py", line 61, in validate_hdfs_path: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO -     raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path "): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] INFO - hcompressor.hdfs_utils.InvalidHdfsPathException: hdfs:///raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd is not a valid hdfs path : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 09:54:05] ERROR - Task failed with exception: source="task"
RuntimeError: Erreur hcompressor (exit=1)
stdout=b'\r\n3785434 | 2026-01-29 09:54:01,395 | hcompressor | INFO | Arguments parsed successfully\r\nTraceback (most recent call last):\r\n  File "./hcompressor", line 22, in <module>\r\n    hcompressor.hcompressor.main()\r\n  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 72, in main\r\n    dfs.validate_hdfs_path(input_path)\r\n  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py", line 61, in validate_hdfs_path\r\n    raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path ")\r\nhcompressor.hdfs_utils.InvalidHdfsPathException: hdfs:///raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd is not a valid hdfs path \r\n'
stderr=b''
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 128 in decompress_if_needed



[cdprct@cdp-gateway-01-rct bin]$ cd /opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py
-bash: cd: /opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py: Not a directory
[cdprct@cdp-gateway-01-rct bin]$ cd ..
[cdprct@cdp-gateway-01-rct lib]$ cd .
[cdprct@cdp-gateway-01-rct lib]$ cd /opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py
-bash: cd: /opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py: Not a directory
