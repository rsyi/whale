from datetime import datetime
from pathlib import Path
import logging
from whalebuilder.utils.task_wrappers import create_and_run_tasks_from_yaml
from whalebuilder.utils.paths import ETL_LOG_PATH, LOGS_DIR

Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
Path(ETL_LOG_PATH).touch()
logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)
logging.basicConfig(
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=ETL_LOG_PATH,
    filemode="w",
    level=logging.INFO)


LOGGER = logging.getLogger("whalebuilder")
LOGGER.addHandler(logging.StreamHandler())

start_time = datetime.now()
LOGGER.info("ETL process started.")

create_and_run_tasks_from_yaml()

end_time = datetime.now()
LOGGER.info("All tasks completed.")
time_elapsed = end_time - start_time
LOGGER.info("Total time elapsed: {}".format(str(time_elapsed)))
