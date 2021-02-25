from typing import (  # noqa: F401
    Any,
    Dict,
    Iterator,
    Union,
)

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401

from whale.extractor.base_postgres_metadata_extractor import (
    BasePostgresMetadataExtractor,
)
from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata
from itertools import groupby


class PostgresMetadataExtractor(BasePostgresMetadataExtractor):
    """
    Extracts Postgres table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """

    def get_sql_statement(
        self, use_catalog_as_cluster_name: bool, where_clause_suffix: str
    ) -> str:
        if use_catalog_as_cluster_name:
            cluster_source = "c.table_catalog"
        else:
            cluster_source = f"'{self._cluster}'"

        return """
        SELECT
            {cluster_source} as cluster, c.table_schema as schema, c.table_name as name, pgtd.description as description
          , c.column_name as col_name, c.data_type
          , pgcd.description as col_description, ordinal_position as col_sort_order
          , CASE WHEN b.table_name IS NOT NULL THEN 1 ELSE 0 END AS is_view
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN -- Deviating from amundsen to include view tables
          pg_catalog.pg_statio_all_tables as st on c.table_schema=st.schemaname and c.table_name=st.relname
        LEFT JOIN
          pg_catalog.pg_description pgcd on pgcd.objoid=st.relid and pgcd.objsubid=c.ordinal_position
        LEFT JOIN
          pg_catalog.pg_description pgtd on pgtd.objoid=st.relid and pgtd.objsubid=0
        LEFT JOIN INFORMATION_SCHEMA.VIEWS b ON c.table_catalog = b.table_catalog
            and c.table_schema = b.table_schema
            and c.table_name = b.table_name

        {where_clause_suffix}
        ORDER by cluster, schema, name, col_sort_order ;
        """.format(
            cluster_source=cluster_source,
            where_clause_suffix=where_clause_suffix,
        )

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(
                    ColumnMetadata(
                        row["col_name"],
                        row["col_description"],
                        row["data_type"],
                        row["col_sort_order"],
                    )
                )

            # Deviating from amundsen to add `is_view`
            yield TableMetadata(
                self._database,
                last_row["cluster"],
                last_row["schema"],
                last_row["name"],
                last_row["description"],
                columns,
                last_row["is_view"],
            )

    def get_scope(self) -> str:
        return "extractor.postgres_metadata"
