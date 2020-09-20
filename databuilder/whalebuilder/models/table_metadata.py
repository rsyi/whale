import copy

from typing import Iterable, List, Optional


class ColumnMetadata:
    COLUMN_KEY_FORMAT = '{db}://{catalog}.{schema}.{tbl}/{col}'

    def __init__(self,
                 name: str,
                 description: Optional[str],
                 col_type: str,
                 sort_order: int,
                 tags: Optional[List[str]] = None,
                 is_partition_column: Optional[bool] = None,
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
        return 'ColumnMetadata({!r}, {!r}, {!r}, {!r}, {!r})' \
                .format(
                    self.name,
                    self.description,
                    self.type,
                    self.sort_order,
                    self.is_partition_column)


class TableMetadata(object):
    """
    Simplified table metadata that contains columns, extended with
    markdown_blob.
    """
    TABLE_KEY_FORMAT = '{db}://{catalog}.{schema}.{tbl}'

    DATABASE_KEY_FORMAT = 'database://{db}'

    CATALOG_KEY_FORMAT = '{db}://{catalog}'

    def __init__(
            self,
            database: str,
            catalog: str,
            schema: str,
            name: str,
            description: Optional[str] = None,
            columns: Iterable[ColumnMetadata] = None,
            is_view: bool = False,
            markdown_blob: str = '',
            tags: Optional[List] = None,
            description_source: Optional[str] = None,
            **kwargs
            ):
        # type: (...) -> None
        """
        :param database:
        :param catalog:
        :param schema:
        :param name:
        :param description:
        :param columns:
        :param is_view:
        :param tags:
        :param kwargs:
        """
        self.database = database
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.description = description
        self.columns = columns if columns else []
        self.is_view = is_view
        self.markdown_blob = markdown_blob
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
                                                self.catalog,
                                                self.schema,
                                                self.name,
                                                self.description,
                                                self.columns,
                                                self.is_view,
                                                self.tags)

    def _get_table_key(self):
        # type: () -> str
        return TableMetadata.TABLE_KEY_FORMAT.format(
            db=self.database,
            catalog=self.catalog,
            schema=self.schema,
            tbl=self.name)

    def _get_database_key(self):
        # type: () -> str
        return TableMetadata.DATABASE_KEY_FORMAT.format(db=self.database)

    def _get_catalog_key(self):
        # type: () -> str
        return TableMetadata.CATALOG_KEY_FORMAT.format(
            db=self.database,
            catalog=self.catalog)

    def _get_col_key(self, col):
        # type: (ColumnMetadata) -> str
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(
            db=self.database,
            catalog=self.catalog,
            schema=self.schema,
            tbl=self.name,
            col=col.name)


class TableColumnStats:
    """
    Table stats model.
    """
    LABEL = 'Stat'
    KEY_FORMAT = '{db}://{catalog}.{schema}' \
                 '.{table}/{col}/{stat_name}/'
    STAT_Column_RELATION_TYPE = 'STAT_OF'
    Column_STAT_RELATION_TYPE = 'STAT'

    def __init__(self,
            table_name: str,
            col_name: str,
            stat_name: str,
            stat_val: str,
            start_epoch: str,
            end_epoch: str,
            db: str = 'hive',
            catalog: str = 'gold',
            schema: str = None
    ):
        if schema is None:
            self.schema, self.table = table_name.split('.')
        else:
            self.table = table_name.lower()
            self.schema = schema.lower()
        self.db = db
        self.col_name = col_name.lower()
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.catalog = catalog
        self.stat_name = stat_name
        self.stat_val = stat_val

    def get_table_stat_model_key(self):
        # type: (...) -> str
        return TableColumnStats.KEY_FORMAT.format(db=self.db,
            catalog=self.catalog,
            schema=self.schema,
            table=self.table,
            col=self.col_name,
            stat_name=self.stat_name)

    def get_col_key(self):
        # type: (...) -> str
        # no catalog, schema info from the input
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(db=self.db,
            catalog=self.catalog,
            schema=self.schema,
            tbl=self.table,
            col=self.col_name)
