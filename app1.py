Log message source details: sources=["/opt/airflow/logs/dag_id=dag_rattrapage/run_id=asset_triggered__2026-01-28T13:19:09.339181+00:00_lQOJKEvg/task_id=process_file.decompress_if_needed/map_index=0/attempt=2.log"]
[2026-01-28, 14:35:57] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-28, 14:35:57] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-28, 14:35:57] WARNING - /home/airflow/.local/lib/python3.12/site-packages/airflow/models/connection.py:471: DeprecationWarning: Using Connection.get_connection_from_secrets from `airflow.models` is deprecated.Please use `get` on Connection from sdk(`airflow.sdk.Connection`) instead
  warnings.warn(
: source="py.warnings"
[2026-01-28, 14:35:57] INFO - Connection Retrieved 'SSH_REC_RATTRAPAGE': source="airflow.hooks.base"
[2026-01-28, 14:35:57] WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-28, 14:35:57] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-28, 14:36:01] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-28, 14:36:01] INFO - Authentication (password) failed.: source="paramiko.transport"
[2026-01-28, 14:36:01] INFO - Failed to connect. Sleeping before retry attempt 1: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-28, 14:36:04] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-28, 14:36:06] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-28, 14:36:06] INFO - Authentication (password) failed.: source="paramiko.transport"
[2026-01-28, 14:36:06] INFO - Failed to connect. Sleeping before retry attempt 2: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-28, 14:36:10] INFO - Connected (version 2.0, client OpenSSH_8.7): source="paramiko.transport"
[2026-01-28, 14:36:14] INFO - Auth banner: b'---------------------------!! ATTENTION !!-----------------------------------\n\nAttention : vous etes maintenant connectes a un systeme informatique securise\n\nde ATTIJARIWAFA BANK. Toutes les actions menees sur ce systeme sont monitorees.\n\n-----------------------------------------------------------------------------\n': source="paramiko.transport"
[2026-01-28, 14:36:14] INFO - Authentication (password) failed.: source="paramiko.transport"
[2026-01-28, 14:36:14] ERROR - Task failed with exception: source="task"
AuthenticationException: Authentication failed.
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 102 in decompress_if_needed

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/ssh/hooks/ssh.py", line 333 in get_conn

File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 445 in __iter__

File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 378 in iter

File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 420 in exc_check

File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 187 in reraise

File "/usr/local/lib/python3.12/concurrent/futures/_base.py", line 449 in result

File "/usr/local/lib/python3.12/concurrent/futures/_base.py", line 401 in __get_result

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/ssh/hooks/ssh.py", line 340 in get_conn

File "/home/airflow/.local/lib/python3.12/site-packages/paramiko/client.py", line 485 in connect

File "/home/airflow/.local/lib/python3.12/site-packages/paramiko/client.py", line 818 in _auth

File "/home/airflow/.local/lib/python3.12/site-packages/paramiko/client.py", line 805 in _auth

File "/home/airflow/.local/lib/python3.12/site-packages/paramiko/transport.py", line 1638 in auth_password

File "/home/airflow/.local/lib/python3.12/site-packages/paramiko/auth_handler.py", line 263 in wait_for_response
