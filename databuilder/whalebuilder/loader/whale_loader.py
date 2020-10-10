import os
import yaml

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
import re
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from whalebuilder.utils import (
    create_base_table_stub,
    get_table_file_path_base,
    get_table_file_path_relative,
    safe_write
)
from whalebuilder.utils.paths import TMP_MANIFEST_PATH
from whalebuilder.utils.parsers import (
    parse_programmatic_blob,
    parse_ugc,
    markdown_from_sections,
    sections_from_markdown,
)
import whalebuilder.models.table_metadata as metadata_model_whale
from whalebuilder.models.metric_value import MetricValue
from databuilder.models.watermark import Watermark

from whalebuilder.utils.markdown_delimiters import (
    COLUMN_DETAILS_DELIMITER,
    METRICS_DELIMITER,
    PARTITIONS_DELIMITER,
)

from whalebuilder.utils.parsers import (
    HEADER_SECTION,
    COLUMN_DETAILS_SECTION,
    PARTITION_SECTION,
    UGC_SECTION,
    METRICS_SECTION
)



class WhaleLoader(Loader):
    """
    Loader class to format metadata as as a markdown doc for whale.
    """
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        'base_directory': os.path.join(Path.home(), '.whale/metadata/'),
        'tmp_manifest_path': TMP_MANIFEST_PATH,
    })

    def init(self, conf: ConfigTree):
        self.conf = conf.with_fallback(WhaleLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string('base_directory')
        self.tmp_manifest_path = self.conf.get_string('tmp_manifest_path', None)
        self.database_name = self.conf.get_string('database_name', None)
        Path(self.base_directory).mkdir(parents=True, exist_ok=True)

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
        database = self.database_name or record.database

        if cluster == "None":  # edge case for Hive Metastore
            cluster = None

        table_file_path_base = get_table_file_path_base(
            database=database,
            cluster=cluster,
            schema=schema,
            table=table,
            base_directory=self.conf.get('base_directory')
        )

        file_path = table_file_path_base + '.md'
        subdirectory = '/'.join(file_path.split('/')[:-1])
        Path(subdirectory).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(file_path):
            create_base_table_stub(
                file_path=file_path,
                database=database,
                cluster=cluster,
                schema=schema,
                table=table)

        self.update_markdown(file_path, record)

        if self.tmp_manifest_path is not None:
            self._append_to_temp_manifest(
                database=database,
                cluster=cluster,
                schema=schema,
                table=table,
                tmp_manifest_path=self.tmp_manifest_path)

    def update_markdown(self, file_path, record):
        # Key (on record type) functions that take actions on a table stub
        section_methods = {
            MetricValue: self._update_metric,
            Watermark: self._update_watermark
        }

        sections = sections_from_markdown(file_path)
        # The table metadata record has both a header and column details. Add
        # custom logic to handle both.
        if type(record) == metadata_model_whale.TableMetadata:
            table_details = \
                re.split(COLUMN_DETAILS_DELIMITER, record.markdown_blob)
            header = table_details[0]
            column_details = "".join(table_details[1:])
            sections[HEADER_SECTION] = header
            # Since we split on COLUMN_DETAILS_DELIMITER, reintroduce it
            sections[COLUMN_DETAILS_SECTION] = \
                COLUMN_DETAILS_DELIMITER + column_details + "\n"
        else:
            section_method = section_methods[type(record)]
            sections = section_method(sections, record)

        new_file_text = markdown_from_sections(sections)
        safe_write(file_path, new_file_text)

    def _update_watermark(self, sections, record):
        part_type = 'high' if record.part_type=='high_watermark' \
            else 'low'
        section_to_update = sections[PARTITION_SECTION]

        existing_watermarks = self._get_data_from_section(section_to_update, PARTITIONS_DELIMITER)

        for part in record.parts:
            name, value = part
            if name not in existing_watermarks:
                existing_watermarks[name] = {}
            existing_watermarks[name][part_type] = value

        sections[PARTITION_SECTION] = PARTITIONS_DELIMITER + "\n```\n" \
            + self._get_section_from_watermarks(existing_watermarks) + "```\n"
        return sections

    def _get_data_from_section(self, section, delimiter):
        # Remove the delimiter
        if section:
            section = section.split(delimiter)[0]
            if "```" in section:
                sections_split_by_backtick = section.split("```")
                section = "\n".join(sections_split_by_backtick)
            data = yaml.safe_load(section)
            if data is None:
                data = {}
        else:
            data = {}
        return data

    def _get_section_from_watermarks(self, watermarks):
        section = yaml.dump(watermarks)
        return section

    def _update_metric(self, sections, record):
        section_to_update = sections[METRICS_SECTION]
        existing_metrics = self._get_metrics_from_section(section_to_update)

        existing_metrics[record.name] = {
            "execution_time": record.execution_time,
            "value": record.value
        }
        new_section = self._get_section_from_metrics(existing_metrics)
        sections[METRICS_SECTION] = new_section
        return sections

    def _get_metrics_from_section(self, section):
        metrics_dict = {}
        raw_metrics_dict = self._get_data_from_section(section, METRICS_DELIMITER)
        for metric_name, value_string in raw_metrics_dict.items():
            payload = value_string.split("@")

            metrics_dict[metric_name] = {
                "execution_time": payload[1].strip(),
                "value": payload[0].strip()
            }
        return metrics_dict

    def _get_section_from_metrics(self, metrics):
        markdown_blobs = [METRICS_DELIMITER + "\n"]
        for metric, metric_details in metrics.items():
            markdown_blob = \
                f"{metric}: {metric_details['value']} @ {metric_details['execution_time']}\n"
            markdown_blobs.append(markdown_blob)
        return "".join(markdown_blobs)

    def _append_to_temp_manifest(
            self,
            database,
            cluster,
            schema,
            table,
            tmp_manifest_path=TMP_MANIFEST_PATH):
        relative_file_path = get_table_file_path_relative(
            database,
            cluster,
            schema,
            table
        )
        with open(tmp_manifest_path, "a") as f:
            f.write(relative_file_path + "\n")


    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.whale"
