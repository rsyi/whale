import argparse
import logging
from whalebuilder.utils.task_wrappers import create_and_run_tasks_from_yaml
logging.basicConfig(format='%(message)s', level=logging.INFO)
logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)

print("Starting the extraction process.")
parser = argparse.ArgumentParser()
parser.add_argument('--no-cache', action='store_true')
args = parser.parse_args()
create_and_run_tasks_from_yaml(
    is_full_extraction_enabled=args.no_cache)
