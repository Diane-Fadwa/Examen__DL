import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml
from airflow.sdk import DAG, Asset, task, task_group

from awb_lib.providers.knox.hooks.knox_livy_hook import KnoxLivyHook
from awb_lib.providers.knox.hooks.knox_webhdfs_hook import KnoxWebHDFSHook
from config import (
    DAG_START_DATE,
    OCP_DAGS_FOLDER_PREFIX,
    PUT_HDFS_POOL,
)

# Import helper functions from dag_factory_refactored
from dags.ingestion.dag_factory_utils import (
    collect_triggered_files,
    upload_file_to_hdfs,
)

logger = logging.getLogger(__name__)


def load_contract_configs(contracts_path: str) -> List[Dict[str, Any]]:
    """
    Load all contract YAML files from workflow directories.

    :param contracts_path: Base path containing workflow definition directories
    :return: List of contract configurations
    """
    parent = Path(contracts_path)
    workflow_directories = [p for p in parent.iterdir() if p.is_dir()]

    contracts = []
    for workflow_dir in workflow_directories:
        # Look for contract files (contract_*.yml or contract_*.yaml)
        contract_files = list(workflow_dir.glob("contract_*.yml")) + list(
            workflow_dir.glob("contract_*.yaml")
        )

        for contract_file in contract_files:
            try:
                with contract_file.open("r", encoding="utf-8") as f:
                    contract_data = yaml.safe_load(f)

                    # Add file path metadata for reference
                    contract_data["_source_file"] = str(contract_file)
                    contract_data["_workflow_dir"] = workflow_dir.name

                    contracts.append(contract_data)
                    logger.info("Loaded contract from: %s", contract_file)
            except Exception as e:
                logger.error("Failed to load contract from %s: %s", contract_file, e)

    logger.info("Successfully loaded %d contract configurations", len(contracts))
    return contracts


def parse_asset_metadata(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse asset section from contract.

    :param contract: Full contract dictionary
    :return: Parsed asset metadata
    """
    asset = contract.get("asset", {})

    return {
        "asset_id": asset.get("id"),
        "asset_name": asset.get("name"),
        "domain": asset.get("domain"),
        "context": asset.get("context"),
        "type": asset.get("type"),
        "description": asset.get("description", "").strip(),
        "version": asset.get("version"),
        "status": asset.get("status"),
        "tags": asset.get("tags", []),
        "business_owner": asset.get("owners", {}).get("business_owner", {}),
        "technical_owner": asset.get("owners", {}).get("technical_owner", {}),
    }


def parse_contract_schema(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse contract section including schema and data quality rules.

    :param contract: Full contract dictionary
    :return: Parsed contract metadata
    """
    contract_section = contract.get("contract", {})

    # Parse schema fields
    fields = []
    for field in contract_section.get("schema", {}).get("fields", []):
        fields.append(
            {
                "name": field.get("name"),
                "type": field.get("type"),
                "description": field.get("description"),
                "required": field.get("required", False),
                "pii_classification": field.get("pii_classification", "none"),
                "business_rules": field.get("business_rules", []),
                "extra_properties": field.get("extra_properties", {}),
            }
        )

    return {
        "primary_key": contract_section.get("primary_key", []),
        "grain": contract_section.get("grain"),
        "refresh_frequency": contract_section.get("refresh", {}).get("frequency"),
        "sla": contract_section.get("sla", {}),
        "schema_fields": fields,
        "field_count": len(fields),
        "field_names": [f["name"] for f in fields],
    }


def parse_quality_rules(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse quality checks and failure policies.

    :param contract: Full contract dictionary
    :return: Parsed quality metadata
    """
    quality = contract.get("quality", {})

    checks = []
    for check in quality.get("checks", []):
        checks.append(
            {
                "name": check.get("name"),
                "type": check.get("type"),
                "field": check.get("field"),
                "threshold": check.get("threshold"),
                "critical": check.get("critical", False),
                "expression": check.get("expression"),
            }
        )

    return {
        "checks": checks,
        "check_count": len(checks),
        "critical_checks": [c for c in checks if c.get("critical")],
        "on_failure_action": quality.get("on_failure", {}).get("action"),
        "notification_emails": quality.get("on_failure", {}).get("notify", []),
    }


def parse_security_policies(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse security classification and access policies.

    :param contract: Full contract dictionary
    :return: Parsed security metadata
    """
    security = contract.get("security", {})

    return {
        "classification": security.get("classification"),
        "contains_pii": security.get("pii", {}).get("contains_pii", False),
        "pii_notes": security.get("pii", {}).get("notes"),
        "access_roles": security.get("access_policies", {}).get("roles", []),
        "row_level_filters_enabled": security.get("access_policies", {})
        .get("row_level_filters", {})
        .get("enabled", False),
        "column_masking_rules": security.get("access_policies", {}).get(
            "column_masking", []
        ),
    }


def parse_ingestion_config(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse ingestion-specific configuration from extra_properties.

    :param contract: Full contract dictionary
    :return: Parsed ingestion configuration
    """
    # In the new contract structure, ingestion properties are directly in extra_properties
    # Not nested under extra_properties.ingestion
    extra_props = contract.get("extra_properties", {})

    # Get asset name as fallback for workflow_name
    asset_name = contract.get("asset", {}).get("name")

    return {
        "workflow_name": asset_name,  # Use asset name as workflow name
        "header_contains_date": extra_props.get("header_contains_date", False),
        "date_position": extra_props.get("date_position", -1),
        "number_header": extra_props.get("number_header", 0),
        "number_footer": extra_props.get("number_footer", 0),
        "separator": extra_props.get("separator", ","),
        "queue": extra_props.get("queue", "default"),
        "env": extra_props.get("env", "dev"),
    }


def parse_input_output_config(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse input sources and output configuration.

    :param contract: Full contract dictionary
    :return: Parsed input/output metadata
    """
    inputs = contract.get("inputs", {})
    output = contract.get("output", {})

    return {
        "sources": inputs.get("sources", []),
        "transformations": inputs.get("transformations", []),
        "output_table": output.get("table_name"),
        "storage_format": output.get("storage_format"),
        "location": output.get("location"),
        "partitioning": output.get("partitioning", []),
        "retention": output.get("retention", {}),
    }


def parse_operations_config(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse operational configuration (scheduling, retries, etc.).

    :param contract: Full contract dictionary
    :return: Parsed operations metadata
    """
    operations = contract.get("operations", {})

    return {
        "dag_id": operations.get("airflow_dag_id"),
        "schedule_cron": operations.get("schedule_cron"),
        "expected_runtime_minutes": operations.get("expected_runtime_minutes"),
        "alerts_channel": operations.get("alerts_channel"),
        "dependencies": operations.get("dependencies", []),
        "max_retries": operations.get("retries", {}).get("max_retries", 1),
        "backoff_minutes": operations.get("retries", {}).get("backoff_minutes", 5),
        "log_level": operations.get("logging", {}).get("level", "INFO"),
        "log_retention_days": operations.get("logging", {}).get(
            "log_retention_days", 30
        ),
    }


def parse_lineage(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse data lineage information.

    :param contract: Full contract dictionary
    :return: Parsed lineage metadata
    """
    lineage = contract.get("lineage", {})

    return {
        "upstream_sources": lineage.get("upstream", []),
        "downstream_consumers": lineage.get("downstream", []),
        "documentation_links": lineage.get("documentation_links", []),
    }


def create_contract_based_dag(
    contract: Dict[str, Any],
    default_args: Dict[str, Any],
) -> DAG:
    """
    Create a DAG from a contract configuration.

    :param contract: Contract configuration dictionary
    :param default_args: Default DAG arguments
    :return: DAG instance
    """
    # Parse all sections
    asset_metadata = parse_asset_metadata(contract)
    contract_schema = parse_contract_schema(contract)
    ingestion_config = parse_ingestion_config(contract)
    io_config = parse_input_output_config(contract)
    operations_config = parse_operations_config(contract)

    # Extract ingestion parameters
    workflow_name = ingestion_config["workflow_name"]
    env = ingestion_config["env"]
    queue = ingestion_config["queue"]

    from config import NAMENODE

    namenode = f"hdfs://{NAMENODE}"

    # Build HDFS paths for artifacts
    artifacts_dir = f"/awb_{env}/awb_ingestion/artifacts/{workflow_name}"
    contract_hdfs_path = f"{artifacts_dir}/contract_{workflow_name}.yml"
    pyspark_script_hdfs_path = f"{artifacts_dir}/check_meta_from_contract.py"
    pyspark_script_local = (
        f"{OCP_DAGS_FOLDER_PREFIX}/scripts/check_meta_from_contract.py"
    )

    # Determine DAG ID from operations config or asset name
    dag_id = (
        operations_config.get("dag_id") or f"contract_{asset_metadata['asset_name']}"
    )
    schedule = [Asset(workflow_name)]

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=asset_metadata.get(
            "description", f"Contract-based ingestion DAG for {workflow_name}"
        ),
        schedule=schedule,
        start_date=datetime.strptime(DAG_START_DATE, "%Y-%m-%d"),
        catchup=True,
        tags=asset_metadata.get("tags", []),
    ) as dag:

        @task
        def collect_files():
            """Collect files from asset events."""
            return collect_triggered_files(
                workflow_name,
                ingestion_config["number_header"],
                ingestion_config["number_footer"],
                ingestion_config["separator"],
            )

        @task
        def upload_artifacts_to_hdfs():
            """Upload contract and PySpark script to HDFS (once, not per file)."""
            webhdfs_hook = KnoxWebHDFSHook(conn_id="KNOX_REC")

            logger.info(f"Creating artifacts directory: {artifacts_dir}")
            webhdfs_hook.create_directory(artifacts_dir, overwrite=True)

            # Build contract local path using OCP prefix
            workflow_dir = contract.get("_workflow_dir")

            # Construct the contract path relative to OCP_DAGS_FOLDER_PREFIX
            contract_local_path = f"{OCP_DAGS_FOLDER_PREFIX}/dags/ingestion/dags_definitions/{workflow_dir}/contract_{workflow_name}.yml"

            logger.info(
                f"Uploading contract: {contract_local_path} -> {contract_hdfs_path}"
            )
            webhdfs_hook.upload_file(
                contract_local_path, contract_hdfs_path, overwrite=True
            )

            logger.info(
                f"Uploading PySpark script: {pyspark_script_local} -> {pyspark_script_hdfs_path}"
            )
            webhdfs_hook.upload_file(
                pyspark_script_local, pyspark_script_hdfs_path, overwrite=True
            )

            return {
                "contract_path": contract_hdfs_path,
                "script_path": pyspark_script_hdfs_path,
            }

        @task_group
        def process_file(
            wf_name: str,
            file_path: str,
            archive_path: str,
            raw_dir: str,
            skip_header: int,
            skip_footer: int,
            separator: str,
        ):
            """Process a single file through the ingestion pipeline."""

            @task(pool=PUT_HDFS_POOL)
            def upload_task(file_path, raw_dir, archive_path):
                """Upload file to HDFS with temporary processing suffix."""
                from datetime import date

                metadata_output = {"date": date.today().strftime("%Y%m%d")}

                return upload_file_to_hdfs(
                    file_path, raw_dir, archive_path, metadata_output
                )

            @task
            def spark_validate_ingest(upload_result, file_path, raw_dir):
                """Execute PySpark validation and ingestion via Livy."""
                import os
                from datetime import datetime

                # Use the workflow_name from the closure (contract-level)
                wf_name = workflow_name

                # Build HDFS file path from raw_dir and file_path
                filename = os.path.basename(file_path)

                # Check if upload_result contains the path
                if "hdfs_file_path" in upload_result:
                    hdfs_file_path = upload_result["hdfs_file_path"]
                elif "output_file" in upload_result:
                    hdfs_file_path = upload_result["output_file"]
                else:
                    # Fallback: construct from raw_dir
                    hdfs_file_path = f"{raw_dir.rstrip('/')}/{filename}"

                logger.info(f"Using HDFS file path: {hdfs_file_path}")

                # Initialize Livy hook
                livy_hook = KnoxLivyHook(conn_id="KNOX_REC")

                # Create unique job name using timestamp and filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:19]
                # Add a random component for additional uniqueness
                import random

                random_suffix = random.randint(1000, 9999)
                job_name = f"contract_ingestion_{wf_name}_{timestamp}_{random_suffix}"

                logger.info(f"Submitting Spark job: {job_name}")
                logger.info(f"Workflow: {wf_name}")
                logger.info(f"Contract path: {contract_hdfs_path}")
                logger.info(f"Input file: {hdfs_file_path}")

                # Submit batch job
                batch_id = livy_hook.post_batch(
                    file=f"{namenode}{pyspark_script_hdfs_path}",
                    name=job_name,
                    args=[
                        f"{namenode}{contract_hdfs_path}",
                        f"{hdfs_file_path}",
                    ],
                    queue=queue,
                    conf={
                        "spark.sql.sources.partitionOverwriteMode": "dynamic",
                        "spark.sql.adaptive.enabled": "true",
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": "2",
                        "spark.dynamicAllocation.maxExecutors": "3",
                    },
                    driver_memory="1g",
                    driver_cores=1,
                    executor_memory="2g",
                    executor_cores=2,
                    num_executors=2,
                )

                logger.info(f"Batch submitted with ID: {batch_id}")

                # Poll for completion
                final_state = livy_hook.poll_for_completion(
                    session_id=batch_id,
                    polling_interval=30,
                    max_polling_attempts=120,
                )

                logger.info(f"Spark job completed with state: {final_state}")

                return {
                    "batch_id": batch_id,
                    "final_state": final_state.value,
                    "workflow_name": wf_name,
                    "job_name": job_name,
                }

            # Define task dependencies within the group
            upload_result = upload_task(file_path, raw_dir, archive_path)
            spark_validate_ingest(upload_result, file_path, raw_dir)

        @task
        def cleanup_artifacts(process_results):
            """Remove contract and script from HDFS after all files are processed."""
            webhdfs_hook = KnoxWebHDFSHook(conn_id="KNOX_REC")

            logger.info(f"Cleaning up artifacts directory: {artifacts_dir}")
            # logger.info(f"All files processed: {len(process_results)} files")  # Remove this line

            webhdfs_hook.delete_path(artifacts_dir, recursive=True)

            return {
                "status": "cleaned",
                "artifacts_dir": artifacts_dir,
                # "files_processed": len(process_results),  # Remove this line
            }

        # Define DAG-level task flow
        files = collect_files()
        artifacts = upload_artifacts_to_hdfs()

        # Process all files (dynamically mapped)
        process_results = process_file.expand_kwargs(files)

        # Cleanup only after all files are processed
        cleanup = cleanup_artifacts(process_results)

        # Define dependencies
        # 1. Upload artifacts after collecting files
        files >> artifacts

        # 2. Start processing files only after artifacts are uploaded
        artifacts >> process_results

        # 3. Cleanup only after all file processing is complete
        process_results >> cleanup

    return dag


def instantiate_dags_from_contracts(contracts: List[Dict[str, Any]]) -> Dict[str, DAG]:
    """
    Create DAG instances from contract configurations.

    :param contracts: List of contract dictionaries
    :return: Dictionary of DAG instances
    """
    dags = {}

    for contract in contracts:
        try:
            asset = contract.get("asset", {})
            asset_name = asset.get("name", "unknown")

            dag_instance = create_contract_based_dag(
                contract=contract,
                default_args={
                    "owner": asset.get("owners", {})
                    .get("technical_owner", {})
                    .get("name", "airflow"),
                    "depends_on_past": False,
                    "retries": contract.get("operations", {})
                    .get("retries", {})
                    .get("max_retries", 1),
                },
            )

            # Use DAG ID from contract or generate one
            operations = contract.get("operations", {})
            dag_key = operations.get("airflow_dag_id") or f"contract_{asset_name}"

            dags[dag_key] = dag_instance
            logger.info("Created DAG instance: %s", dag_key)

        except Exception as e:
            logger.error(
                "Failed to create DAG for contract %s: %s",
                contract.get("_source_file", "unknown"),
                e,
                exc_info=True,
            )

    return dags


# ============================================================================
# Main Execution - Load contracts and create DAGs
# ============================================================================

CONTRACTS_PATH = f"{OCP_DAGS_FOLDER_PREFIX}/dags/ingestion/dags_definitions"

logger.info("Loading contract configurations from: %s", CONTRACTS_PATH)
contract_configs = load_contract_configs(CONTRACTS_PATH)
logger.info("Loaded %d contract configurations", len(contract_configs))

logger.info("Instantiating DAGs from contracts...")
created_dags = instantiate_dags_from_contracts(contract_configs)
logger.info("Created %d DAG instances", len(created_dags))

# Register DAGs in global namespace
globals().update(created_dags)

logger.info("Contract-based DAG factory initialization complete")
