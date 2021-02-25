import copy
from typing import Dict, Iterable, List, Optional, Union
import textwrap
from databuilder.models.table_metadata import DescriptionMetadata
from whale.models.column_metadata import ColumnMetadata
from whale.utils.markdown_delimiters import COLUMN_DETAILS_DELIMITER


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
        **kwargs,
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

    def format_for_markdown(self):
        block_template = textwrap.dedent(
            """        # `{schema_statement}{name}`{view_statement}
        `{database}`{cluster_statement}
        {description}
        {column_details_delimiter}
        {columns}
            """
        )

        formatted_columns_list = [
            column.format_for_markdown() for column in self.columns
        ]
        formatted_columns = "\n".join(formatted_columns_list)

        if self.description:
            if type(self.description) == DescriptionMetadata:
                description = self.description._text + "\n"
            else:
                description = str(self.description) + "\n"
        else:
            description = ""

        if self.cluster == "None":  # edge case for Hive Metastore
            cluster = None
        else:
            cluster = self.cluster

        if cluster is not None:
            cluster_statement = f" | `{cluster}`"
        else:
            cluster_statement = ""

        if (
            self.schema == None
        ):  # edge case for Glue, which puts everything in self.table
            schema_statement = ""
        else:
            schema_statement = f"{self.schema}."

        markdown_blob = block_template.format(
            schema_statement=schema_statement,
            name=self.name,
            view_statement=" [view]" if self.is_view else "",
            database=self.database,
            cluster_statement=cluster_statement,
            description=description,
            column_details_delimiter=COLUMN_DETAILS_DELIMITER,
            columns=formatted_columns,
        )

        return markdown_blob

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
