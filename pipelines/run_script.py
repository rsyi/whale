from whale import execute_markdown_sql_blocks, execute_sql_file

import argparse
import pathlib

parser = argparse.ArgumentParser(description="Run a query.")
parser.add_argument("filename")
parser.add_argument("--w", dest="warehouse_name", default=None)
args = parser.parse_args()

# This is an API accessed by the rust client with two behaviors:
#  * For markdown files, search for sql blocks with `--!wh-execute` and run
#    them, saving the results in-line.
#  * For SQL files, execute them and print the results here. If `--!wh-execute`
#    is included, also save the results to the file.

file_extension = pathlib.Path(args.filename).suffix
if file_extension.lower() in [".md", ".markdown"]:
    result = execute_markdown_sql_blocks(args.filename)
else:  # Catchall for all sql-based extensions
    result = execute_sql_file(args.filename, warehouse_name=args.warehouse_name)
print(result)
