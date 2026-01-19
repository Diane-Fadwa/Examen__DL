Log message source details: sources=["/opt/airflow/logs/dag_id=dag_rattrapage/run_id=asset_triggered__2026-01-19T08:35:19.400128+00:00_vzP6SbOt/task_id=validate_rattrapage_payload/attempt=1.log"]
[2026-01-19, 09:35:26] INFO - DAG bundles loaded: dags-folder: source="airflow.dag_processing.bundles.manager.DagBundlesManager"
[2026-01-19, 09:35:26] INFO - Filling up the DagBag from /opt/airflow/dags/repo/rattrapage/dag_rattrapage.py: source="airflow.models.dagbag.DagBag"
[2026-01-19, 09:35:27] INFO - Triggering asset Asset(name='replay://rattrapage', uri='replay://rattrapage/', group='asset', extra={}, watchers=[]) payload: {'contract_path': 'Hey1', 'files': 'raw', 'from_rest_api': True}: source="unusual_prefix_eb329238963d72514d1fc4fc57b48c5803517fb6_dag_rattrapage"
[2026-01-19, 09:35:27] ERROR - Task failed with exception: source="task"
KeyError: 'asset_events'
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 920 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1215 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/decorator.py", line 251 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 397 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 216 in execute

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/python.py", line 239 in execute_callable

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/callback_runner.py", line 81 in run

File "/opt/airflow/dags/repo/rattrapage/dag_rattrapage.py", line 48 in validate_rattrapage_payload

[2026-01-19, 09:35:35] ERROR - Top level error: source="task"
AirflowRuntimeError: API_SERVER_ERROR: {'status_code': 404, 'message': 'Server returned error', 'detail': {'detail': {'reason': 'not_found', 'message': 'Task Instance not found'}}}
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1351 in main

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 999 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 204 in send

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 264 in _get_response

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/comms.py", line 251 in _from_frame

[2026-01-19, 09:35:35] WARNING - Process exited abnormally: exit_code=1: source="task"


"contract_path":"Hey2"
"files":"raw"
