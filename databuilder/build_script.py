import argparse
from datetime import datetime
import logging
from whalebuilder.utils.task_wrappers import create_and_run_tasks_from_yaml
from whalebuilder.utils.paths import ETL_LOG_PATH

logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)
logging.basicConfig(
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=ETL_LOG_PATH,
    filemode="w",
    level=logging.INFO)


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler())

start_time = datetime.now()
LOGGER.info("ETL process started.")

parser = argparse.ArgumentParser()
# parser.add_argument("--no-cache", action="store_true")  # accessible as args.no_cache
# args = parser.parse_args()
create_and_run_tasks_from_yaml()

end_time = datetime.now()
LOGGER.info("All tasks completed.")
time_elapsed = end_time - start_time
LOGGER.info("Total time elapsed: {}".format(str(time_elapsed)))
