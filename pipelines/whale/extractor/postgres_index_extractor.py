from typing import Iterator, Union, Dict, Any
from collections import namedtuple
from itertools import groupby

from whale.extractor.base_index_extractor import IndexExtractor
from whale.models.index_metadata import TableIndexesMetadata, IndexMetadata

TableKey = namedtuple("TableKey", ["schema", "name"])
IndexKey = namedtuple("IndexKey", ["schema", "table", "name"])


class PostgresIndexExtractor(IndexExtractor):
    def _get_sql_statement(
        self, use_catalog_as_cluster_name: bool, where_clause_suffix: str = ""
    ) -> str:

        cluster_source = (
            "c.table_catalog" if use_catalog_as_cluster_name else self._cluster
        )

        if where_clause_suffix == "":
            where_clause_suffix = "WHERE TRUE"

        return """
        SELECT
            {cluster_source} AS cluster,
            n.nspname AS schema,
            t.relname AS table,
            i.relname AS index_name,
            ix.indisunique AS is_unique,
            ix.indisprimary AS is_primary,
            ix.indisclustered AS is_clustered,
            a.attname AS column_name

        FROM pg_catalog.pg_class t
        JOIN
          pg_catalog.pg_index ix ON t.oid = ix.indrelid
        JOIN
          pg_catalog.pg_class i ON i.oid = ix.indexrelid
        JOIN
          pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN
          pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        JOIN
          information_schema.columns c ON c.table_schema=n.nspname AND c.table_name = t.relname AND c.column_name = a.attname

        {where_clause_suffix}
        AND t.relkind = 'r'
        ORDER BY t.relname, i.relname;
        """.format(
            cluster_source=cluster_source,
            where_clause_suffix=where_clause_suffix,
        )

    def _get_extract_iter(self):
        for table_key, table_group in groupby(
            self._get_raw_extract_iter(), self._get_table_key
        ):

            indexes = []
            for index_key, index_group in groupby(table_group, self._get_index_key):
                columns = []

                for row in index_group:
                    last_row = row
                    columns.append(row["column_name"])

                indexes.append(
                    IndexMetadata(
                        name=last_row["index_name"],
                        index_type="primary" if last_row["is_primary"] else None,
                        architecture="clustered" if last_row["is_clustered"] else None,
                        constraint="unique" if last_row["is_unique"] else None,
                        columns=columns,
                    )
                )

            yield TableIndexesMetadata(
                database=self._database,
                cluster=last_row["cluster"],
                schema=last_row["schema"],
                table=last_row["table"],
                indexes=indexes,
            )

    def _get_table_key(self, row: Dict[str, Any]) -> Union[TableKey, None]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if not row:
            return None

        return TableKey(
            schema=row["schema"],
            name=row["table"],
        )

    def _get_index_key(self, row: Dict[str, Any]) -> Union[IndexKey, None]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if not row:
            return None

        return IndexKey(
            schema=row["schema"],
            table=row["table"],
            name=row["index_name"],
        )
