[2026-01-29, 10:56:19] INFO - : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:19] INFO - 3836458 | 2026-01-29 10:56:19,801 | hcompressor | INFO | Arguments parsed successfully: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO - Traceback (most recent call last):: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -   File "./hcompressor", line 22, in <module>: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -     hcompressor.hcompressor.main(): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 72, in main: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -     dfs.validate_hdfs_path(input_path): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -   File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hdfs_utils.py", line 61, in validate_hdfs_path: source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO -     raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path "): source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] INFO - hcompressor.hdfs_utils.InvalidHdfsPathException: /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.zstd is not a valid hdfs path : source="airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook"
[2026-01-29, 10:56:23] ERROR - Task failed with exception: source="task"
RuntimeError: Erreur hcompressor (exit=1)
stdout=
3836458 | 2026-01-29 10:56:19,801 | hcompressor | INFO | Arguments parsed successfully
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

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 127 in decompress_if_needed
