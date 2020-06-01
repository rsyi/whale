import argparse
import logging
from metaframe import main
logging.basicConfig(format='%(message)s', level=logging.INFO)
logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument('--no-cache', action='store_true')
args = parser.parse_args()
main(is_full_extraction_enabled=args.no_cache)
