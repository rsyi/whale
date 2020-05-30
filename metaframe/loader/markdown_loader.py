import csv
import logging
import os
import textwrap

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from tabulate import tabulate
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from databuilder.models.table_metadata import TableMetadata


class MarkdownLoader(Loader):
    """
    Loader class to format metadata as as a markdown doc for metaframe.
    """
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        'base_directory': os.path.join(Path.home(), '.metaframe/metadata/')
    })

    TABLE_RELATIVE_FILE_PATH = '{database}/{cluster}.{schema}.{table}.md'

    METAFRAME_HEADER_TEMPLATE = textwrap.dedent("""    {schema}.{name} {view_statement}
    """)
    METAFRAME_DOC_TEMPLATE = textwrap.dedent("""    {header}
    Database: {database} | Cluster: {cluster} | Schema: {schema}

    # Description
    {description}

    # Columns
    {columns}
    """)

    def init(self, conf):
        # type: (ConfigTree) -> None
        """
        Initialize file handlers from conf
        :param conf:
        """
        self.conf = conf.with_fallback(MarkdownLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string('base_directory')
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
        if type(record) == TableMetadata:
            description = record.description
            columns = record.columns
            rows = [['column', 'type', 'description']]
            for column in columns:
                rows.append([column.name, column.type, column.description])
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

            # Make file (and directory, if it doesn't exist).
            relative_file_path = MarkdownLoader.TABLE_RELATIVE_FILE_PATH.format(
                database=record.database,
                cluster=record.cluster,
                schema=record.schema,
                table=record.name
            )
            relative_file_path_docs = relative_file_path[:-3] + '.docs.md'
            absolute_file_path=os.path.join(self.base_directory, relative_file_path)
            absolute_file_path_docs=os.path.join(self.base_directory, relative_file_path_docs)

            relative_subdirectory = relative_file_path.split('/')[0]
            absolute_subdirectory = os.path.join(self.base_directory, relative_subdirectory)
            Path(absolute_subdirectory).mkdir(parents=True, exist_ok=True)

            Path(absolute_file_path_docs).touch()
            with open(absolute_file_path, 'w') as f:
                f.write(metaframe_docs)

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.markdown"

