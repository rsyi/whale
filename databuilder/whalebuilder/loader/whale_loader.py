import os

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
import re
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from whalebuilder.utils import get_table_file_path_base, safe_write
from whalebuilder.transformer.markdown_transformer import FormatterMixin
import whalebuilder.models.table_metadata as metadata_model_whale

HEADER_SECTION = 'header'
COLUMN_DETAILS_SECTION = 'column_details'
PARTITION_SECTION = 'partition'
USAGE_SECTION = 'usage'
COLUMN_DETAILS_DELIMITER = "## Column Details"
PARTITIONS_DELIMITER = "## Partition info"
USAGE_DELIMITER = "## Usage info"


def _parse_programmatic_blob(programmatic_blob):

    regex_to_match = "(" + COLUMN_DETAILS_DELIMITER + "|" + partitions_delimiter + "|" + usage_delimiter + ")"


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
        elif clause == partitions_delimiter:
            state = PARTITION_SECTION
        elif clause == usage_delimiter:
            state = USAGE_SECTION

        sections[state].append(clause)

    for state, clauses in sections.items():
        sections[state] = "".join(clauses)
    return sections


def sections_from_markdown(file_path):

    with open(file_path, "r") as f:
        old_file_text = "".join(f.readlines())

    file_strings = old_file_text.split(FormatterMixin.UGC_DELIMITER)

    programmatic_blob = file_strings[0]

    programmatic_sections = _parse_programmatic_blob(programmatic_blob)

    ugc = "".join(file_strings[1:])

    sections = {
        "ugc": ugc,
    }
    sections.update(programmatic_sections)
    return sections


def markdown_from_sections(
        header_blob="",
        column_details_blob="",
        partition_blob="",
        usage_blob="",
        ugc_blob="\n# README",
        ):

    programmatic_blob = "\n".join([
        header_blob,
        column_details_blob,
        partition_blob,
        usage_blob])

    final_blob = FormatterMixin.UGC_DELIMITER.join([programmatic_blob, ugc_blob])
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


    def load(self, record):
        # type: (Any) -> None
        """
        Write record object as csv to file
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

        Path(file_path).touch()
        self.modify_section(file_path, record)


    def modify_section(self, file_path, record):
        # Key (on record type) functions that take actions on a table stub
        section_dict = {
        }

        sections = sections_from_markdown(file_path)

        # The table metadata record has both a header and column details. Add
        # custom logic to handle both.
        if type(record) == metadata_model_whale.TableMetadata:
            table_details = re.split(COLUMN_DETAILS_DELIMITER, record.markdown_blob)
            header = table_details[0]
            column_details = "".join(table_details[1:])
            sections[HEADER_SECTION] = header
            sections[COLUMN_DETAILS_SECTION] = column_details
        else:
            section_to_update = section_dict[type(record)]
            sections[section_to_update] = record.markdown_blob + "\n"

        new_file_text = markdown_from_sections(sections)
        safe_write(file_path, new_file_text)


    def sync_manifest(self, manifest):
        pass

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.whale"
