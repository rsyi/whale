from typing import Dict, Iterable, List, Optional, Union
from whale.utils.markdown_delimiters import COLUMN_DETAILS_DELIMITER


class ColumnMetadata:
    COLUMN_KEY_FORMAT = "{db}://{cluster}.{schema}.{tbl}/{col}"

    def __init__(
        self,
        name: str,
        description: Optional[str],
        data_type: str,
        sort_order: int,
        tags: Optional[Union[List[Dict], List[str]]] = None,
        is_partition_column: Optional[bool] = None,
    ):
        # type: (...) -> None
        """
        :param name: Name of the column
        :param description: Description of the column
        :param data_type: Data type of the column, e.g. integer or bool.
        :param sort_order: Rank of the column
        """
        self.name = name
        self.description = description
        self.type = data_type
        self.sort_order = sort_order
        self.tags = tags
        self.is_partition_column = is_partition_column


    def format_for_markdown(self):
        max_type_length = 9

        self.template_no_desc = "* {buffered_type} `{name}`"
        self.template = self.template_no_desc + "\n  - {description}"

        buffer_length = max(max_type_length - len(self.type), 0)
        buffered_type = "[" + self.type + "]" + " " * buffer_length

        if self.description:
            return self.template.format(
                buffered_type=buffered_type,
                name=self.name,
                description=self.description,
            )
        else:
            return self.template_no_desc.format(
                buffered_type=buffered_type,
                name=self.name,
            )


    def __repr__(self):
        # type: () -> str
        return "ColumnMetadata({!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.name,
            self.description,
            self.type,
            self.sort_order,
            self.tags,
            self.is_partition_column,
        )
