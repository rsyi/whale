import os
from pathlib import Path

TABLE_RELATIVE_FILE_PATH = '{database}/{catalog}.{schema}.{table}'
CLUSTERLESS_TABLE_RELATIVE_FILE_PATH = '{database}/{schema}.{table}'


def get_table_file_path_base(
        database,
        catalog,
        schema,
        table,
        base_directory=os.path.join(Path.home(), '.metaframe/metadata/')
        ):

    if catalog is not None:
        relative_file_path = TABLE_RELATIVE_FILE_PATH.format(
            database=database,
            catalog=catalog,
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
