import os

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


def _append_to_temp_manifest(record):
    relative_file_path = get_table_file_path_relative(
        record.database,
        record.cluster,
        record.schema,
        record.name
    )
    with open(TMP_MANIFEST_PATH, "a") as f:
        f.write(relative_file_path + "\n")


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
        'base_directory': os.path.join(Path.home(), '.whale/metadata/')
    })

    def init(self, conf: ConfigTree):
        self.conf = conf.with_fallback(WhaleLoader.DEFAULT_CONFIG)
        self.base_directory = self.conf.get_string('base_directory')
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

        table_file_path_base = get_table_file_path_base(
            database=self.database_name or record.database,
            cluster=record.cluster,
            schema=record.schema,
            table=record.name,
            base_directory=self.conf.get('base_directory')
        )

        file_path = table_file_path_base + '.md'
        subdirectory = '/'.join(file_path.split('/')[:-1])
        Path(subdirectory).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(file_path):
            create_base_table_stub(
                file_path,
                record.database,
                record.cluster,
                record.schema,
                record.name)
        Path(TMP_MANIFEST_PATH).touch()

        self.update_markdown(file_path, record)
        _append_to_temp_manifest(record)

    def update_markdown(self, file_path, record):
        # Key (on record type) functions that take actions on a table stub
        section_dict = {
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
                COLUMN_DETAILS_DELIMITER + column_details
        else:
            section_to_update = section_dict[type(record)]
            sections[section_to_update] = record.markdown_blob + "\n"

        new_file_text = markdown_from_sections(sections)
        safe_write(file_path, new_file_text)

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.whale"
