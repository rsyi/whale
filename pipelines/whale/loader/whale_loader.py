import os
import yaml

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
import re
import textwrap
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from whale.utils import (
    create_base_table_stub,
    get_table_file_path_base,
    get_table_file_path_relative,
    safe_write,
)
from whale.utils import paths
from whale.utils.parsers import (
    parse_programmatic_blob,
    parse_ugc,
    markdown_from_sections,
    sections_from_markdown,
)
import whale.models.table_metadata as metadata_model_whale
from whale.models.metric_value import MetricValue
from databuilder.models.watermark import Watermark
from databuilder.models.table_metadata import DescriptionMetadata
import databuilder.models.table_metadata as metadata_model_amundsen

from whale.utils.markdown_delimiters import (
    COLUMN_DETAILS_DELIMITER,
    METRICS_DELIMITER,
    PARTITIONS_DELIMITER,
    TAGS_DELIMITER,
)

from whale.utils.parsers import (
    HEADER_SECTION,
    COLUMN_DETAILS_SECTION,
    PARTITION_SECTION,
    UGC_SECTION,
    METRICS_SECTION,
)


class WhaleLoader(Loader):
    """
    Loader class to format metadata as as a markdown doc for whale.
    """

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            "base_directory": paths.METADATA_PATH,
            "tmp_manifest_path": paths.TMP_MANIFEST_PATH,
        }
    )

    def init(self, conf: ConfigTree):
        self.conf = conf.with_fallback(WhaleLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string("base_directory")
        self.tmp_manifest_path = self.conf.get_string("tmp_manifest_path", None)
        self.database_name = self.conf.get_string("database_name", None)
        Path(self.base_directory).mkdir(parents=True, exist_ok=True)
        Path(paths.MANIFEST_DIR).mkdir(parents=True, exist_ok=True)

    def load(self, record) -> None:
        """
        Creates a table stub if it does not exist, updates this template with
        information in `record`.
        :param record:
        :return:
        """
        if not record:
            return

        if type(record) in [MetricValue, Watermark]:
            table = record.table
        else:
            table = record.name

        schema = record.schema
        cluster = record.cluster
        if (
            "/" in record.database
        ):  # TODO: In general, we should always use self.database_name, unless we override the amundsen extractor and add subdirectories
            database = record.database
        else:  # ... so we have to do this.
            database = self.database_name or record.database

        if cluster == "None":  # edge case for Hive Metastore
            cluster = None

        table_file_path_base = get_table_file_path_base(
            database=database,
            cluster=cluster,
            schema=schema,
            table=table,
            base_directory=self.conf.get("base_directory"),
        )

        file_path = table_file_path_base + ".md"
        subdirectory = "/".join(file_path.split("/")[:-1])
        Path(subdirectory).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(file_path):
            create_base_table_stub(
                file_path=file_path,
                database=database,
                cluster=cluster,
                schema=schema,
                table=table,
            )

        update_markdown(file_path, record)

        if self.tmp_manifest_path is not None:
            _append_to_temp_manifest(
                database=database,
                cluster=cluster,
                schema=schema,
                table=table,
                tmp_manifest_path=self.tmp_manifest_path,
            )

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.whale"


def update_markdown(file_path, record):
    # Key (on record type) functions that take actions on a table stub
    section_methods = {
        MetricValue: _update_metric,
        Watermark: _update_watermark,
        metadata_model_whale.TableMetadata: _update_table_metadata,
        metadata_model_amundsen.TableMetadata: _update_table_metadata,
    }

    sections = sections_from_markdown(file_path)
    # The table metadata record has both a header and column details. Add
    # custom logic to handle both.
    section_method = section_methods[type(record)]
    sections = section_method(sections, record)

    new_file_text = markdown_from_sections(sections)
    safe_write(file_path, new_file_text)


def format_yaml_section(section, delimiter):
    return delimiter + "\n```\n" + section + "```\n"


def _update_watermark(sections, record):
    part_type = "high" if record.part_type == "high_watermark" else "low"
    section_to_update = sections[PARTITION_SECTION]

    existing_watermarks = _get_data_from_section(
        section_to_update, PARTITIONS_DELIMITER
    )

    for part in record.parts:
        name, value = part
        if name not in existing_watermarks:
            existing_watermarks[name] = {}
        existing_watermarks[name][part_type] = value

    section = _get_section_from_watermarks(existing_watermarks)
    sections[PARTITION_SECTION] = format_yaml_section(section, PARTITIONS_DELIMITER)
    return sections


def _get_data_from_section(section, delimiter):
    # Remove the delimiter
    if section:
        section = section.split(delimiter)[-1]
        if "```" in section:
            sections_split_by_backtick = section.split("```")
            section = "\n".join(sections_split_by_backtick)
        data = yaml.safe_load(section)
        if data is None:
            data = {}
    else:
        data = {}
    return data


def _get_section_from_watermarks(watermarks):
    section = yaml.dump(watermarks)
    return section


def _update_table_metadata(sections, record):
    table_metadata_blob = format_table_metadata(record)

    table_details = re.split(COLUMN_DETAILS_DELIMITER, table_metadata_blob)
    header = table_details[0]
    column_details = "".join(table_details[1:])
    sections[HEADER_SECTION] = header
    # Since we split on COLUMN_DETAILS_DELIMITER, reintroduce it
    sections[COLUMN_DETAILS_SECTION] = COLUMN_DETAILS_DELIMITER + column_details + "\n"

    # tag_blob = format_tags(record)
    # sections[TAGS_DELIMITER] = (
    #     TAGS_DELIMITER + tag_blob + "\n"
    # )

    return sections


def _update_metric(sections, record):
    section_to_update = sections[METRICS_SECTION]
    metrics_dict = _get_metrics_from_section(section_to_update)

    metrics_dict[record.name] = {
        "execution_time": record.execution_time,
        "value": record.value,
    }
    new_section = _get_section_from_metrics(metrics_dict)
    sections[METRICS_SECTION] = format_yaml_section(new_section, METRICS_DELIMITER)
    return sections


def format_table_metadata(record) -> metadata_model_whale.TableMetadata:
    block_template = textwrap.dedent(
        """        # `{schema_statement}{name}`{view_statement}
        `{database}`{cluster_statement}
        {description}
        {column_details_delimiter}
        {columns}
        """
    )

    formatted_columns = format_columns(record)

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

    return markdown_blob


def format_columns(record) -> str:
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


def _get_metrics_from_section(section):
    metrics_dict = {}
    raw_metrics_dict = _get_data_from_section(section, METRICS_DELIMITER)
    for metric_name, value_string in raw_metrics_dict.items():
        payload = value_string.split("@")

        metrics_dict[metric_name] = {
            "execution_time": payload[1].strip(),
            "value": payload[0].strip(),
        }
    return metrics_dict


def _get_section_from_metrics(metrics):
    markdown_blobs = []
    for metric, metric_details in metrics.items():
        markdown_blob = f"{metric}: {metric_details['value']} @ {metric_details['execution_time']}\n"
        markdown_blobs.append(markdown_blob)
    return "".join(markdown_blobs)


def _append_to_temp_manifest(
    database, cluster, schema, table, tmp_manifest_path=paths.TMP_MANIFEST_PATH
):
    relative_file_path = get_table_file_path_relative(database, cluster, schema, table)
    with open(tmp_manifest_path, "a") as f:
        f.write(relative_file_path + "\n")
