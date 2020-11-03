from whale import run

import argparse

parser = argparse.ArgumentParser(description="Run a query.")
parser.add_argument("filename")
parser.add_argument("--w", dest="warehouse_name", default=None)
args = parser.parse_args()

with open(args.filename, "r") as f:
    sql = f.read()

result = run(sql=sql, warehouse_name=args.warehouse_name)
print(result)
