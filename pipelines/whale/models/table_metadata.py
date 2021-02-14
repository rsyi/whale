import copy
from typing import Dict, Iterable, List, Optional, Union
from whale.models.column_metadata import ColumnMetadata

class TableMetadata(object):
    """
    Simplified table metadata that contains columns, extended with
    markdown_blob.
    """

    TABLE_KEY_FORMAT = "{db}://{cluster}.{schema}.{tbl}"

    DATABASE_KEY_FORMAT = "database://{db}"

    CATALOG_KEY_FORMAT = "{db}://{cluster}"

    def __init__(
        self,
        database: str,
        cluster: str,
        schema: str,
        name: str,
        description: Optional[str] = None,
        columns: Iterable[ColumnMetadata] = None,
        is_view: bool = False,
        markdown_blob: str = "",
        tags: Optional[Union[List[Dict], List[str]]] = None,
        labels: Optional[Union[Dict, List[str]]] = None,
        description_source: Optional[str] = None,
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
        :param labels:
        :param kwargs:
        """
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.description = description
        self.columns = columns if columns else []
        self.is_view = is_view
        self.markdown_blob = markdown_blob
        self.attrs = None
        if isinstance(tags, str):
            tags = list(filter(None, tags.split(",")))
        if isinstance(tags, list):
            tags = [tag.lower().strip() for tag in tags]
        self.tags = tags
        self.labels = labels

        if kwargs:
            self.attrs = copy.deepcopy(kwargs)

    def __repr__(self):
        # type: () -> str
        return "TableMetadata({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.database,
            self.cluster,
            self.schema,
            self.name,
            self.description,
            self.columns,
            self.is_view,
            self.tags,
            self.labels,
        )

    def _get_table_key(self):
        # type: () -> str
        return TableMetadata.TABLE_KEY_FORMAT.format(
            db=self.database, cluster=self.cluster, schema=self.schema, tbl=self.name
        )

    def _get_database_key(self):
        # type: () -> str
        return TableMetadata.DATABASE_KEY_FORMAT.format(db=self.database)

    def _get_cluster_key(self):
        # type: () -> str
        return TableMetadata.CATALOG_KEY_FORMAT.format(
            db=self.database, cluster=self.cluster
        )

    def _get_col_key(self, col):
        # type: (ColumnMetadata) -> str
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(
            db=self.database,
            cluster=self.cluster,
            schema=self.schema,
            tbl=self.name,
            col=col.name,
        )


