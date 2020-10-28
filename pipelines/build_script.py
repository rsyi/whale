from datetime import datetime
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from whale.utils.task_wrappers import pull
from whale.utils.paths import ETL_LOG_PATH, LOGS_DIR

Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
stream_handler = logging.StreamHandler()
rotating_handler = RotatingFileHandler(
    str(ETL_LOG_PATH), maxBytes=50 * 1024 * 1024, backupCount=5
)
logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    handlers=[rotating_handler, stream_handler],
    level=logging.INFO,
)

LOGGER = logging.getLogger("whale")
LOGGER.info("ETL process started.")

start_time = datetime.now()
pull()

end_time = datetime.now()
LOGGER.info("All tasks completed.")
time_elapsed = end_time - start_time
LOGGER.info("Total time elapsed: {}".format(str(time_elapsed)))
