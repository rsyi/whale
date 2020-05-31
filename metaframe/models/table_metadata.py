import copy
from collections import namedtuple

from typing import Iterable, Any, Union, Iterator, List, Dict, Set  # noqa: F401

from databuilder.models.cluster import cluster_constants

DESCRIPTION_NODE_LABEL_VAL = 'Description'
DESCRIPTION_NODE_LABEL = DESCRIPTION_NODE_LABEL_VAL


class ColumnMetadata:
    COLUMN_KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}/{col}'

    def __init__(self,
                 name: str,
                 description: Union[str, None],
                 col_type: str,
                 sort_order: int,
                 tags: Union[List[str], None] = None,
                 is_partition_column: Union[bool, None] = None,
                 ):
        # type: (...) -> None
        """
        :param name:
        :param description:
        :param col_type:
        :param sort_order:
        """
        self.name = name
        self.description = description
        self.type = col_type
        self.sort_order = sort_order
        self.tags = tags
        self.is_partition_column = is_partition_column

    def __repr__(self):
        # type: () -> str
        return 'ColumnMetadata({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.name,
                                                               self.description,
                                                               self.type,
                                                               self.sort_order,
                                                               self.is_partition_column)


class TableMetadata(object):
    """
    Simplified table metadata that contains columns.
    """
    TABLE_KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}'

    DATABASE_KEY_FORMAT = 'database://{db}'

    CLUSTER_KEY_FORMAT = '{db}://{cluster}'

    def __init__(self,
                 database: str,
                 cluster: str,
                 schema: str,
                 name: str,
                 description: Union[str, None],
                 columns: Iterable[ColumnMetadata] = None,
                 is_view: bool = False,
                 tags: Union[List, str] = None,
                 description_source: Union[str, None] = None,
                 **kwargs
                 ):
        # type: (...) -> None
        """
        :param database:
        :param cluster:
        :param schema:
        :param name:
        :param description:
        :param columns:
        :param is_view:
        :param tags:
        :param kwargs:
        """
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.description = description
        self.columns = columns if columns else []
        self.is_view = is_view
        self.attrs = None
        if isinstance(tags, str):
            tags = list(filter(None, tags.split(',')))
        if isinstance(tags, list):
            tags = [tag.lower().strip() for tag in tags]
        self.tags = tags

        if kwargs:
            self.attrs = copy.deepcopy(kwargs)

    def __repr__(self):
        # type: () -> str
        return 'TableMetadata({!r}, {!r}, {!r}, {!r} ' \
               '{!r}, {!r}, {!r}, {!r})'.format(self.database,
                                                self.cluster,
                                                self.schema,
                                                self.name,
                                                self.description,
                                                self.columns,
                                                self.is_view,
                                                self.tags)

    def _get_table_key(self):
        # type: () -> str
        return TableMetadata.TABLE_KEY_FORMAT.format(db=self.database,
                                                     cluster=self.cluster,
                                                     schema=self.schema,
                                                     tbl=self.name)

    def _get_database_key(self):
        # type: () -> str
        return TableMetadata.DATABASE_KEY_FORMAT.format(db=self.database)

    def _get_cluster_key(self):
        # type: () -> str
        return TableMetadata.CLUSTER_KEY_FORMAT.format(db=self.database,
                                                       cluster=self.cluster)

    def _get_col_key(self, col):
        # type: (ColumnMetadata) -> str
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(db=self.database,
                                                       cluster=self.cluster,
                                                       schema=self.schema,
                                                       tbl=self.name,
                                                       col=col.name)

