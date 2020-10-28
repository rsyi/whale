from typing import Iterable, List, Optional  # noqa: F401


class TableManifest(object):
    """
    Table manifest that contains only a list of all tables
    """

    def __init__(
        self,
        tables: list,
        markdown_blob: str,
    ):
        self.tables = tables
        self.markdown_blob = markdown_blob
