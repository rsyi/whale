from databuilder.transformer.base_transformer import Transformer
import whale.models.table_metadata as metadata_model_whale
import databuilder.models.table_metadata as metadata_model_amundsen
from whale.models.metric_value import MetricValue
from whale.utils.markdown_delimiters import COLUMN_DETAILS_DELIMITER
from databuilder.models.watermark import Watermark
from databuilder.models.table_metadata import DescriptionMetadata

import textwrap


class FormatterMixin:
    def format_table_metadata(self, record) -> metadata_model_whale.TableMetadata:
        block_template = textwrap.dedent(
            """            # `{schema_statement}{name}`{view_statement}
            `{database}`{cluster_statement}
            {description}
            {column_details_delimiter}
            {columns}
            """
        )

        formatted_columns = self.format_columns(record)

        if record.description:
            if type(record.description) == DescriptionMetadata:
                description = record.description._text + "\n"
            else:
                description = str(record.description) + "\n"
        else:
            description = ""

        if record.cluster == "None":  # edge case for Hive Metastore
            cluster = None
        else:
            cluster = record.cluster

        if cluster is not None:
            cluster_statement = f" | `{cluster}`"
        else:
            cluster_statement = ""

        if (
            record.schema == None
        ):  # edge case for Glue, which puts everything in record.table
            schema_statement = ""
        else:
            schema_statement = f"{record.schema}."

        markdown_blob = block_template.format(
            schema_statement=schema_statement,
            name=record.name,
            view_statement=" [view]" if record.is_view else "",
            database=record.database,
            cluster_statement=cluster_statement,
            description=description,
            column_details_delimiter=COLUMN_DETAILS_DELIMITER,
            columns=formatted_columns,
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

        if columns:
            column_template_no_desc = "* {buffered_type} `{name}`"
            column_template = column_template_no_desc + "\n  - {description}"
            formatted_columns_list = []

            for column in columns:
                buffer_length = max(max_type_length - len(column.type), 0)
                buffered_type = "[" + column.type + "]" + " " * buffer_length

                if column.description:
                    formatted_column_text = column_template.format(
                        buffered_type=buffered_type,
                        name=column.name,
                        description=column.description,
                    )
                else:
                    formatted_column_text = column_template_no_desc.format(
                        buffered_type=buffered_type,
                        name=column.name,
                    )

                formatted_columns_list.append(formatted_column_text)

            formatted_columns = "\n".join(formatted_columns_list)
            return formatted_columns
        else:
            return ""

    def no_op_format(self, record):
        # No formatting required
        return record


class MarkdownTransformer(Transformer, FormatterMixin):
    """
    Transforms a TableMetadata record into a Markdown string.
    """

    def init(self, conf):
        self.conf = conf
        self.formatters = {
            metadata_model_amundsen.TableMetadata: self.format_table_metadata,
            metadata_model_whale.TableMetadata: self.format_table_metadata,
            Watermark: self.no_op_format,
            MetricValue: self.no_op_format,
        }

    def transform(self, record):
        formatter = self.formatters.get(type(record), None)
        if not record:
            return None
        else:
            return formatter(record)

    def get_scope(self):
        return "transformer.markdown"
