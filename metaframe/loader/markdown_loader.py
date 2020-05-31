import csv
import logging
import os
import textwrap

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from tabulate import tabulate
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
import metaframe.models.table_metadata as metadata_model_metaframe
import databuilder.models.table_metadata as metadata_model_amundsen
from metaframe.utils import get_table_file_path_base

class MarkdownLoader(Loader):
    """
    Loader class to format metadata as as a markdown doc for metaframe.
    """
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        'base_directory': os.path.join(Path.home(), '.metaframe/metadata/')
    })

    METAFRAME_HEADER_TEMPLATE = textwrap.dedent("""    {schema}.{name} {view_statement}
    """)
    METAFRAME_DOC_TEMPLATE = textwrap.dedent("""    {header}
    Database: {database} | Cluster: {cluster} | Schema: {schema}

    # Description
    {description}

    # Columns
    {columns}
    """)

    def init(self, conf: ConfigTree):
        self.conf = conf.with_fallback(MarkdownLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string('base_directory')
        self.database_name = self.conf.get_string('database_name')
        Path(self.base_directory).mkdir(parents=True, exist_ok=True)

    def load(self, record):
        # type: (Any) -> None
        """
        Write record object as csv to file
        :param record:
        :return:
        """
        if not record:
            return
        if type(record) == metadata_model_metaframe.TableMetadata \
                or type(record) == metadata_model_amundsen.TableMetadata:
            description = record.description
            columns = record.columns
            rows = [['column', 'type', 'partition', 'description']]

            for column in columns:
                # Deal with slightly different TableMetadataSchemas.
                if hasattr(column, 'is_partition_column'):
                    if column.is_partition_column:
                        partition_flag = 'x'
                    else:
                        partition_flag = ''
                else:
                    partition_flag = '?'

                rows.append([column.name, column.type, partition_flag, column.description])
                tabulated_columns = tabulate(rows, headers="firstrow", tablefmt="github")

            header = MarkdownLoader.METAFRAME_HEADER_TEMPLATE.format(
                schema=record.schema,
                name=record.name,
                view_statement='[view]' if record.is_view else '',
            )
            header += '-'*(len(record.name) + len(record.schema) + 1)
            metaframe_docs = MarkdownLoader.METAFRAME_DOC_TEMPLATE.format(
                header=header,
                database=record.database,
                cluster=record.cluster,
                schema=record.schema,
                description=description,
                columns=tabulated_columns)

            # Format file names.
            table_file_path_base = get_table_file_path_base(
                database=self.database_name,
                cluster=record.cluster,
                schema=record.schema,
                table=record.name
            )

            file_path = table_file_path_base + '.md'
            file_path_docs = table_file_path_base + '.docs.md'
            subdirectory = '/'.join(file_path.split('/')[:-1])
            Path(subdirectory).mkdir(parents=True, exist_ok=True)

            Path(file_path_docs).touch()
            with open(file_path, 'w') as f:
                f.write(metaframe_docs)

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.markdown"

