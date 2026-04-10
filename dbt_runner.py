from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from typing import Optional # Dibutuhkan untuk validasi parameter Prefect
import os

# --- TASKS ---

@task(name="DBT Connectivity Check")
def check_connectivity(settings: PrefectDbtSettings):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)

    logger.info("Menjalankan 'dbt debug'...")
    try:
        result = runner.invoke(["debug"])
        logger.info(f"Result: {result}")
        if not result.success:
            raise Exception("DBT Debug gagal")
    except Exception as e:
        logger.error("DBT DEBUG CRASHED!")
        logger.error(str(e))
        raise

@task(name="DBT Dependency Validation")
def validate_dependencies(settings: PrefectDbtSettings, selector: Optional[str]):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)
    cmd = ["list"]
    if selector:
        cmd.extend(["--select", selector])
        logger.info(f"Memvalidasi dependensi selector: {selector}")
    
    result = runner.invoke(cmd)
    if result.success:
        logger.info(f"Model terdeteksi:\n{result.result}")
    else:
        raise Exception("Gagal memvalidasi dependensi.")

@task(name="DBT Core Execution")
def execute_dbt_command(settings: PrefectDbtSettings, command: str, selector: Optional[str] = None):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)
    
    full_command = [command]
    if selector:
        full_command.extend(["--select", selector])
    
    logger.info(f"Eksekusi: dbt {' '.join(full_command)}")
    result = runner.invoke(full_command)
    
    if not result.success:
        raise Exception(f"DBT command failed: {result.return_code}")
    return result

# --- FLOW ---

@flow(name="DBT Transformation Runner")
def general_dbt_runner(
    dbt_command: str = "build", 
    analysis_type: Optional[str] = None, # PERBAIKAN: Gunakan Optional[str]
    include_deps: bool = True
):
    logger = get_run_logger()

    project_dir = os.path.join(os.getcwd(), "dbt")
    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=project_dir,
    )

    # Logika Selector
    selector = None
    if analysis_type:
        selector = f"staging.{analysis_type}"
        if include_deps:
            selector += "+"
    elif include_deps:
        selector = "staging+"
    
    # Print env vars untuk debugging
    logger.info(f"Environment Variables:")
    for key in ["STARROCKS_HOST", "STARROCKS_PORT", "STARROCKS_USER", "STARROCKS_PASS", "STARROCKS_DB"]:
        logger.info(f"{key}: {os.getenv(key)}")

    # Eksekusi
    check_connectivity(settings)
    validate_dependencies(settings, selector)
    execute_dbt_command(settings, dbt_command, selector)

    logger.info("Pipeline dbt selesai.")