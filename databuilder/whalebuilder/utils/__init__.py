import os
import shutil
from pathlib import Path
from whalebuilder.utils.markdown_delimiters import UGC_DELIMITER
from whalebuilder.utils.paths import MANIFEST_PATH, TMP_MANIFEST_PATH

TABLE_RELATIVE_FILE_PATH = '{database}/{cluster}.{schema}.{table}'
CLUSTERLESS_TABLE_RELATIVE_FILE_PATH = '{database}/{schema}.{table}'


def get_table_file_path_base(
        database,
        cluster,
        schema,
        table,
        base_directory=os.path.join(Path.home(), '.whale/metadata/')
        ):

    relative_file_path = get_table_file_path_relative(
        database,
        cluster,
        schema,
        table)
    return os.path.join(base_directory, relative_file_path)


def get_table_file_path_relative(
        database,
        cluster,
        schema,
        table):
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
    return relative_file_path


def create_base_table_stub(
        file_path,
        database,
        cluster,
        schema,
        table):
    text_to_write = \
        f"# `{schema}.{table}`\n{database} | {cluster}\n" \
        + "\n" + UGC_DELIMITER \
        + "\n*Do not make edits above this line.*\n"
    safe_write(file_path, text_to_write)


def get_table_info_from_path(
        file_path,
    ):
    database = file_path.parent
    table_string = os.path.dirname(file_path)
    table_components = table_string.split(".")
    table = table_components[-1]
    schema = table_components[-2]
    if len(table_components) == 3:
        cluster = table_components[-3]
    else:
        cluster = None
    return database, cluster, schema, table


def safe_write(file_path_to_write, text_to_write, tmp_extension=".bak"):
    backup_file_path = file_path_to_write + tmp_extension

    with open(backup_file_path, "w") as f:
        f.write(text_to_write)
        f.flush()
        os.fsync(f.fileno())

    os.rename(backup_file_path, file_path_to_write)


def transfer_manifest(tmp_manifest_path):
    os.rename(tmp_manifest_path, MANIFEST_PATH)


def copy_manifest(tmp_manifest_path):
    shutil.copy(tmp_manifest_path, MANIFEST_PATH)
