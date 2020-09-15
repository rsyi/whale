import os

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from metaframe.utils import get_table_file_path_base


class MetaframeLoader(Loader):
    """
    Loader class to format metadata as as a markdown doc for metaframe.
    """
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        'base_directory': os.path.join(Path.home(), '.metaframe/metadata/')
    })

    def init(self, conf: ConfigTree):
        self.conf = conf.with_fallback(MetaframeLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string('base_directory')
        self.database_name = self.conf.get_string('database_name', None)
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

        table_file_path_base = get_table_file_path_base(
            database=self.database_name or record.database,
            catalog=record.catalog,
            schema=record.schema,
            table=record.name,
            base_directory=self.conf.get('base_directory')
        )

        file_path = table_file_path_base + '.md'
        file_path_docs = table_file_path_base + '.docs.md'
        subdirectory = '/'.join(file_path.split('/')[:-1])
        Path(subdirectory).mkdir(parents=True, exist_ok=True)

        Path(file_path_docs).touch()
        with open(file_path, 'w') as f:
            f.write(record.markdown_blob)

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.metaframe"
