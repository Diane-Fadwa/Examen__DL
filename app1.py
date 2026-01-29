[2026-01-29, 12:38:07] WARNING - /home/airflow/.local/lib/python3.12/site-packages/airflow/models/connection.py:471: DeprecationWarning: Using Connection.get_connection_from_secrets from `airflow.models` is deprecated.Please use `get` on Connection from sdk(`airflow.sdk.Connection`) instead
  warnings.warn(
: source="py.warnings"
[2026-01-29, 12:38:07] INFO - Connection Retrieved 'SSH_REC': source="airflow.hooks.base"
[2026-01-29, 12:38:07] WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:38:07] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-29, 12:38:07] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-29, 12:38:07] INFO - Authentication (password) successful!: source="paramiko.transport"
[2026-01-29, 12:38:07] INFO - Décompression via hcompressor : /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-29, 12:38:07] INFO - Running command: 
            cd /opt/workspace/Script/CEKO/hcomp/lib/bin &&             ./hcompressor               --mode decompression               --compression_type zstd               --delete_input 0               /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502
            : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:38:07] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:38:07] INFO - 3917236 | 2026-01-29 12:38:07,428 | hcompressor | INFO | Arguments parsed successfully: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 12:38:17] ERROR - Task failed with exception: source="task"
AirflowException: SSH command timed out
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 117 in decompress_if_needed

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/ssh/hooks/ssh.py", line 503 in exec_ssh_client_command
