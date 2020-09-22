import os
from pathlib import Path
from whalebuilder.transformer.markdown_transformer import FormatterMixin
from whalebuilder.utils.markdown_delimiters import UGC_DELIMITER

TABLE_RELATIVE_FILE_PATH = '{database}/{cluster}.{schema}.{table}'
CLUSTERLESS_TABLE_RELATIVE_FILE_PATH = '{database}/{schema}.{table}'


def get_table_file_path_base(
        database,
        cluster,
        schema,
        table,
        base_directory=os.path.join(Path.home(), '.whale/metadata/')
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


def create_base_table_stub(
        file_path,
        database,
        cluster,
        schema,
        table,
    ):
    text_to_write = f"# `{schema}.{table}`\n{database} | {cluster}\n" \
        + UGC_DELIMITER \
        + "\n*Edits above this line will be overwritten.*\n"
    safe_write(file_path, text_to_write)


def safe_write(file_path_to_write, text_to_write, tmp_extension=".bak"):
    backup_file_path = file_path_to_write + tmp_extension

    with open(backup_file_path, "w") as f:
        f.write(text_to_write)
        f.flush()
        os.fsync(f.fileno())

    os.rename(backup_file_path, file_path_to_write)

