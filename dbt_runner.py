from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from typing import Optional # Dibutuhkan untuk validasi parameter Prefect
import os

# --- TASKS ---

@task(name="DBT Connectivity Check")
def check_connectivity(runner: PrefectDbtRunner):
    logger = get_run_logger()
    logger.info("Menjalankan 'dbt debug'...")
    result = runner.invoke(["debug"])
    if not result.success:
        logger.error(f"DBT Debug Output: {result.result}")
        raise Exception("Koneksi ke database gagal.")

@task(name="DBT Dependency Validation")
def validate_dependencies(runner: PrefectDbtRunner, selector: Optional[str]):
    logger = get_run_logger()
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
def execute_dbt_command(runner: PrefectDbtRunner, command: str, selector: Optional[str] = None):
    logger = get_run_logger()
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
    runner = PrefectDbtRunner(settings=settings)

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
    for key in ["STARROCKS_HOST", "STARROCKS_PORT", "STARROCKS_USER", "STARROCKS_PASSWORD", "STARROCKS_DB"]:
        logger.info(f"{key}: {os.getenv(key)}")

    # Eksekusi
    check_connectivity(runner)
    validate_dependencies(runner, selector)
    execute_dbt_command(runner, dbt_command, selector)

    logger.info("Pipeline dbt selesai.")