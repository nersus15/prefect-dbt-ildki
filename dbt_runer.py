# The Prefect Flow (The "Action")
from prefect import flow, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from typing import str

@flow(name="DBT Transformation Runner")
def general_dbt_runner(dbt_command: str = "build"):
    """
    A general runner for any dbt project. 
    The next programmer only touches SQL files!
    """
    logger = get_run_logger()
    logger.info(f"Starting dbt execution: dbt {dbt_command}")

    # Set the path to the dbt project folder
    settings = PrefectDbtSettings(project_dir="./dbt")
    runner = PrefectDbtRunner(settings=settings)

    # Execute the command (default is 'build', but can be 'run', 'test', etc.)
    # The next programmer can change this in the Prefect UI!
    runner.invoke(dbt_command.split())

if __name__ == "__main__":
    general_dbt_runner()
