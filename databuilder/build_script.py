import argparse
from datetime import datetime
import logging
from whalebuilder.utils.task_wrappers import create_and_run_tasks_from_yaml
from whalebuilder.utils import format_time
logging.basicConfig(format='%(message)s', level=logging.INFO)
logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)


LOGGER = logging.getLogger(__name__)
start_time = datetime.now()
LOGGER.info("[{}] ETL process started.".format(format_time(start_time)))

parser = argparse.ArgumentParser()
parser.add_argument('--no-cache', action='store_true')
args = parser.parse_args()
create_and_run_tasks_from_yaml(
    is_full_extraction_enabled=args.no_cache)

end_time = datetime.now()
LOGGER.info("[{}] All tasks completed.".format(format_time(end_time)))
time_elapsed = end_time - start_time
LOGGER.info("Total time elapsed: {}".format(str(time_elapsed)))
