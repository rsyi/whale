import os

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from typing import Any  # noqa: F401

from databuilder.loader.base_loader import Loader
from whalebuilder.utils import get_table_file_path_base, safe_write
from whalebuilder.transformer.markdown_transformer import FormatterMixin
import whalebuilder.models.table_metadata as metadata_model_whale


def sections_from_markdown(file_path):

    with open(file_path, "r") as f:
        old_file_text = "".join(f.readlines())

    file_strings = old_file_text.split(FormatterMixin.UGC_DEMARCATOR)

    programmatic_blob = file_strings[0]

    # TODO
    programmatic_blob.split("##")

    ugc = "".join(file_strings[1:])

    sections = {
        "table_metadata": None,  # TODO
        "ugc": ugc,
    }
    return sections


def markdown_from_sections(
        table_metadata_blob,
        partition_blob="",
        usage_blob="",
        ugc_blob="\n# README",
        ):

    programmatic_blob = "\n".join([
        table_metadata_blob,
        partition_blob,
        usage_blob])

    final_blob = FormatterMixin.UGC_DEMARCATOR.join([programmatic_blob, ugc_blob])
    return final_blob


class ModifierMixin():

    def modify_table_metadata(self, file_path, record):
        sections = sections_from_markdown(file_path)
        sections['table_metadata'] = record.markdown_blob + "\n"
        new_file_text = markdown_from_sections(sections)
        safe_write(file_path, new_file_text)

    def modify_null(self):
        return None


class WhaleLoader(Loader, ModifierMixin):
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

        # Key (on record type) functions that take actions on a table stub
        self.modifiers = {
            metadata_model_whale.TableMetadata: self.modify_table_metadata,
        }

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

        modifier = self.modifiers.get(type(record), self.modify_null)
        with open(file_path, 'w') as f:
            f.write(record.markdown_blob)

    def sync_manifest(self, manifest):
        pass

    def close(self):
        pass

    def get_scope(self):
        # type: () -> str
        return "loader.metaframe"
