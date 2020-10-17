from whalebuilder import run

import argparse

parser = argparse.ArgumentParser(description='Run a query.')
parser.add_argument('filename')
parser.add_argument('warehouse_name')
args = parser.parse_args()

with open(args.filename, "r") as f:
    sql = f.read()

result = run(sql=sql, warehouse_name=args.warehouse_name)
print(result)
