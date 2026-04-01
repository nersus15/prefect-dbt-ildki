# The Prefect Flow (The "Action")
from prefect import flow, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import os

@flow(name="DBT Transformation Runner")
def general_dbt_runner(dbt_command: str = "build"):
    logger = get_run_logger()

    base_dir = os.getcwd()
    project_dir = os.path.join(base_dir, "dbt")
    profiles_dir = project_dir

    logger.info(f"Running dbt command: {dbt_command}")
    logger.info(f"Project dir: {project_dir}")

    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    runner = PrefectDbtRunner(settings=settings)

    result = runner.invoke(dbt_command.split())

    if result.return_code != 0:
        raise Exception("dbt command failed")
