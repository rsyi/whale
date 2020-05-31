import os
from pathlib import Path

TABLE_RELATIVE_FILE_PATH = '{database}/{cluster}.{schema}.{table}'
CLUSTERLESS_TABLE_RELATIVE_FILE_PATH = '{database}/{schema}.{table}'

def get_table_file_path_base(
        database,
        cluster,
        schema,
        table,
        base_directory=os.path.join(Path.home(), '.metaframe/metadata/')
        ):

    if cluster is not None:
        relative_file_path = TABLE_RELATIVE_FILE_PATH.format(
            database=database,
            cluster=cluster,
            schema=schema,
            table=table
        )
    else:
        relative_file_path = CLUSTERLESS_TABLE_RELATIVE_FILE_PATH.format(
            database=database,
            schema=schema,
            table=table
        )

    return os.path.join(base_directory, relative_file_path)
