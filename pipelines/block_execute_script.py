from whale import execute_adhoc_sql

import argparse

parser = argparse.ArgumentParser(
    description="Execute all query blocks in markdown file ending with --!wh-execute."
)
parser.add_argument("filename")
args = parser.parse_args()

execute_adhoc_sql(args.filename)
