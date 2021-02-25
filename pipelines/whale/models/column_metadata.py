from typing import Dict, Iterable, List, Optional, Union

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


