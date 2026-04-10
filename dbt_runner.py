from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import os

# --- TASKS (Fungsi Kecil untuk Tiap Tahap) ---

@task(name="DBT Connectivity Check")
def check_connectivity(runner: PrefectDbtRunner):
    """Memastikan koneksi ke StarRocks tersedia."""
    logger = get_run_logger()
    logger.info("Menjalankan 'dbt debug' untuk mengecek koneksi...")
    result = runner.invoke(["debug"])
    if not result.success:
        raise Exception("Koneksi ke database gagal. Periksa host/kredensial.")

@task(name="DBT Dependency Validation")
def validate_dependencies(runner: PrefectDbtRunner, selector: str):
    """Mengecek model apa saja yang akan dijalankan."""
    logger = get_run_logger()
    if not selector:
        logger.info("Tidak ada selector khusus, dbt list akan menampilkan seluruh project.")
    
    cmd = ["list"]
    if selector:
        cmd.extend(["--select", selector])
        logger.info(f"Memvalidasi dependensi untuk selector: {selector}")
    
    result = runner.invoke(cmd)
    if result.success:
        logger.info(f"Model yang terdeteksi:\n{result.result}")
    else:
        raise Exception("Gagal memvalidasi dependensi. Cek ref() atau folder path.")

@task(name="DBT Core Execution")
def execute_dbt_command(runner: PrefectDbtRunner, command: str, selector: str = None):
    """Menjalankan perintah dbt utama (run/build)."""
    logger = get_run_logger()
    full_command = [command]
    if selector:
        full_command.extend(["--select", selector])
    
    logger.info(f"Mengeksekusi perintah utama: dbt {' '.join(full_command)}")
    result = runner.invoke(full_command)
    
    if not result.success:
        if result.exception:
            raise result.exception
        raise Exception(f"DBT command failed dengan exit code: {result.return_code}")
    return result

# --- FLOW (Orkestrasi Utama) ---

@flow(name="DBT Transformation Runner")
def general_dbt_runner(dbt_command: str = "build", analysis_type: str = None, include_deps: bool = True):
    logger = get_run_logger()

    # Setup Environment
    project_dir = os.path.join(os.getcwd(), "dbt")
    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=project_dir,
    )
    runner = PrefectDbtRunner(settings=settings)

    # 1. Tentukan Selector
    selector = None
    if analysis_type:
        selector = f"staging.{analysis_type}"
        if include_deps:
            selector += "+"
    elif include_deps:
        selector = "staging+"
        
    # 2. Urutan Eksekusi Modular
    # Langkah 1: Cek Koneksi
    check_connectivity(runner)

    # Langkah 2: Cek Dependensi (Dry-Run)
    validate_dependencies(runner, selector)

    # Langkah 3: Eksekusi Utama
    execute_dbt_command(runner, dbt_command, selector)

    logger.info("Seluruh tahapan pipeline dbt berhasil diselesaikan.")