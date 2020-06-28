from databuilder.transformer.base_transformer import Transformer
import metaframe.models.table_metadata as metadata_model_metaframe
import databuilder.models.table_metadata as metadata_model_amundsen

import textwrap

from tabulate import tabulate


class MarkdownTransformer(Transformer):
    """
    Transforms a TableMetadata record into a Markdown string.
    """

    HEADER_TEMPLATE = '{schema}.{name} {view_statement}'
    SUBHEADER_TEMPLATE = \
        'Database: {database} | Cluster: {catalog}'
    DESCRIPTION_TEMPLATE = textwrap.dedent("""    # Description
    {description}\n
    """)
    METAFRAME_DOC_TEMPLATE = textwrap.dedent("""    {header}
    {subheader}

    {description}{columns}
    """)
    GENERIC_TABLE_HEADER = ['column', 'type', 'description']
    PARTITIONED_TABLE_HEADER = ['column', 'type', 'partition', 'description']

    def init(self, conf):
        self.conf = conf

    def transform(self, record):
        if not record:
            return

        database = record.database
        catalog = record.catalog
        schema = record.schema
        table_name = record.name
        view_statement = '[view]' if record.is_view else ''
        description = record.description

        tabulated_columns = self.parse_columns(record)

        markdown_blob = self.format_templates(
            database=database,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            view_statement=view_statement,
            description=description,
            tabulated_columns=tabulated_columns)

        return metadata_model_metaframe.TableMetadata(
            database=database,
            catalog=catalog,
            schema=schema,
            name=table_name,
            markdown_blob=markdown_blob,
        )

    def parse_columns(self, record):
        columns = record.columns
        columns_processed_for_tabulation = []

        # Check if the extractor is returning metaframe's or amundsen's
        # TableMetadata class. The metaframe class removes all neo4j methods
        # and adds is_partition_column to the ColumnMetadata class.
        # Format all markdown statements.
        record_type = type(record)
        if record_type == metadata_model_metaframe.TableMetadata:
            columns_processed_for_tabulation \
                .append(MarkdownTransformer.PARTITIONED_TABLE_HEADER)
        elif record_type == metadata_model_amundsen.TableMetadata:
            columns_processed_for_tabulation \
                .append(MarkdownTransformer.GENERIC_TABLE_HEADER)

        # Loop through all columns and format ColumnMetadata as a list of
        # lists.
        for column in columns:
            if hasattr(column, 'is_partition_column'):
                if column.is_partition_column:
                    partition_flag = 'x'
                else:
                    partition_flag = ''

                columns_processed_for_tabulation.append([
                    column.name,
                    column.type,
                    partition_flag,
                    column.description])
            else:
                columns_processed_for_tabulation.append([
                    column.name,
                    column.type,
                    column.description])

        if columns_processed_for_tabulation:
            tabulated_columns = \
                tabulate(
                    columns_processed_for_tabulation,
                    headers="firstrow",
                    tablefmt="github")
        else:
            tabulated_columns = ''
        return tabulated_columns

    def format_templates(
            self,
            database,
            catalog,
            schema,
            table_name,
            view_statement,
            description,
            tabulated_columns):

        header = MarkdownTransformer.HEADER_TEMPLATE.format(
            schema=schema,
            name=table_name,
            view_statement=view_statement,
        )
        header += \
            '\n' + '-'*(
                len(table_name) +
                len(schema) +
                len(view_statement) +
                2)
        subheader = MarkdownTransformer.SUBHEADER_TEMPLATE.format(
            database=database,
            catalog=catalog,
        )
        if description is not None:
            description_statement = \
                MarkdownTransformer.DESCRIPTION_TEMPLATE.format(
                    description=description
                )
        else:
            description_statement = ''

        return MarkdownTransformer.METAFRAME_DOC_TEMPLATE.format(
            header=header,
            subheader=subheader,
            description=description_statement,
            columns=tabulated_columns)

    def get_scope(self):
        return "transformer.markdown"
