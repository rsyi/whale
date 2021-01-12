from whale import execute_markdown_sql_blocks

import argparse

parser = argparse.ArgumentParser(
    description="Execute all query blocks in markdown file ending with --!wh-execute."
)
parser.add_argument("filename")
args = parser.parse_args()

execute_markdown_sql_blocks(args.filename)
