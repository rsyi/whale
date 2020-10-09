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
from whalebuilder.utils.markdown_delimiters import (
    COLUMN_DETAILS_DELIMITER,
    PARTITIONS_DELIMITER,
    USAGE_DELIMITER,
    UGC_DELIMITER
)
from whalebuilder.utils.paths import TMP_MANIFEST_PATH
import whalebuilder.models.table_metadata as metadata_model_whale
from databuilder.models.watermark import Watermark

HEADER_SECTION = 'header'
COLUMN_DETAILS_SECTION = 'column_details'
PARTITION_SECTION = 'partition'
USAGE_SECTION = 'usage'
UGC_SECTION = 'ugc'


def _parse_programmatic_blob(programmatic_blob):

    regex_to_match = "(" + COLUMN_DETAILS_DELIMITER \
        + "|" + PARTITIONS_DELIMITER \
        + "|" + USAGE_DELIMITER + ")"

    splits = re.split(regex_to_match, programmatic_blob)

    state = HEADER_SECTION
    sections = {
        HEADER_SECTION: [],
        COLUMN_DETAILS_SECTION: [],
        PARTITION_SECTION: [],
        USAGE_SECTION: [],
    }

    for clause in splits:
        if clause == COLUMN_DETAILS_DELIMITER:
            state = COLUMN_DETAILS_SECTION
        elif clause == PARTITIONS_DELIMITER:
            state = PARTITION_SECTION
        elif clause == USAGE_DELIMITER:
            state = USAGE_SECTION

        sections[state].append(clause)

    for state, clauses in sections.items():
        sections[state] = "".join(clauses)
    return sections


def sections_from_markdown(file_path):

    with open(file_path, "r") as f:
        old_file_text = "".join(f.readlines())

    file_strings = old_file_text.split(UGC_DELIMITER)

    programmatic_blob = file_strings[0]

    programmatic_sections = _parse_programmatic_blob(programmatic_blob)

    ugc = "".join(file_strings[1:])

    sections = {
        UGC_SECTION: ugc,
    }
    sections.update(programmatic_sections)
    return sections


def markdown_from_sections(sections: dict):
    programmatic_blob = sections[HEADER_SECTION] \
        + sections[COLUMN_DETAILS_SECTION]\
        + sections[PARTITION_SECTION]\
        + sections[USAGE_SECTION]

    ugc_blob = sections[UGC_SECTION]
    final_blob = UGC_DELIMITER.join([programmatic_blob, ugc_blob])
    return final_blob


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

        if type(record) == Watermark:
            table = record.table
        else:
            table = record.name

        schema = record.schema
        cluster = record.cluster
        database = self.database_name or record.database

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

        existing_watermarks = self._get_watermarks_from_section(section_to_update)
        if not existing_watermarks:
            existing_watermarks = {}

        for part in record.parts:
            name, value = part
            if name not in existing_watermarks:
                existing_watermarks[name] = {}
            existing_watermarks[name][part_type] = value

        sections[PARTITION_SECTION] = PARTITIONS_DELIMITER + "\n```\n" \
            + self._get_section_from_watermarks(existing_watermarks) + "```\n"
        return sections

    def _get_watermarks_from_section(self, section):
        # Remove the delimiter
        if section:
            section = section.split(PARTITIONS_DELIMITER)[0]
            if "```" in section:
                sections_split_by_backtick = section.split("```")
                section = "\n".join(sections_split_by_backtick)
            watermarks = yaml.safe_load(section)
        else:
            watermarks = {}
        return watermarks

    def _get_section_from_watermarks(self, watermarks):
        section = yaml.dump(watermarks)
        return section

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
