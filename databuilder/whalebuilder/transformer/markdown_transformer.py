from databuilder.transformer.base_transformer import Transformer
import whalebuilder.models.table_metadata as metadata_model_whale
import databuilder.models.table_metadata as metadata_model_amundsen

import textwrap

from tabulate import tabulate

class FormatterMixin():

    UGC_DEMARCATOR = "-"*79

    def format_table_metadata(self, record) -> metadata_model_whale.TableMetadata:
        block_template = textwrap.dedent(
            """            # `{schema}.{name}` {view_statement}
            {database} | {cluster}

            {description}

            {columns}

            {demarcator}
            *Edits above this line will be overwritten.*

            # README
            """)

        formatted_columns = self.format_columns(record)

        markdown_blob = block_template.format(
            schema=record.schema,
            name=record.name,
            view_statement="[view]" if record.is_view else "",
            database=record.database,
            cluster=record.cluster,
            description = record.description,
            columns=formatted_columns,
            demarcator=FormatterMixin.UGC_DEMARCATOR,
        )

        return metadata_model_whale.TableMetadata(
            database=record.database,
            cluster=record.cluster,
            schema=record.schema,
            name=record.name,
            markdown_blob=markdown_blob,
        )

    def format_columns(self, record) -> str:
        max_type_length = 9
        columns = record.columns

        column_template_no_desc = "{buffered_type} `{name}`"
        column_template = \
            column_template_no_desc + "\n - {description}"
        formatted_columns_list = []

        for column in columns:
            buffer_length = max_type_length - len(column.type)
            buffered_type = "[" + column.type + "]" + " "*buffer_length

            if column.description:
                template = column_template
            else:
                template = column_template_no_desc

            formatted_column_text = template.format(
                column_template.format(
                    buffered_type=buffered_type,
                    name=column.name,
                    description=column.description,
                )
            )
            formatted_columns_list.append(formatted_column_text)

        formatted_columns = "\n".join(formatted_columns_list)
        return formatted_columns

    def format_null(self):
        return None


class MarkdownTransformer(Transformer, FormatterMixin):
    """
    Transforms a TableMetadata record into a Markdown string.
    """
    def init(self, conf):
        self.conf = conf
        self.formatters = {
            metadata_model_amundsen.TableMetadata: self.format_table_metadata,
            metadata_model_whale.TableMetadata: self.format_table_metadata,
        }

    def transform(self, record):
        formatter = self.formatters.get(type(record), self.format_null)
        if not record:
            return None
        else:
            return formatter(record)

    def get_scope(self):
        return "transformer.markdown"
