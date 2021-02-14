from typing import Any, Dict, List, Union  # noqa: F401

from whale.models.column_metadata import ColumnMetadata


class PrestoWatermark(object):
    # type: (...) -> None
    """
    Table watermark result model.
    Each instance represents one row of table watermark result.
    """
    KEY_FORMAT = "{database}://{cluster}.{schema}" "/{table}/{part_type}/"

    def __init__(
        self,
        database: str,
        schema: str,
        table_name: str,
        parts: list,
        part_type: str = "high_watermark",
        cluster: str = "gold",
    ) -> None:
        self.database = database.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()

        # currently we don't consider nested partitions
        self.parts = parts
        self.part_type = part_type.lower()
        self.cluster = cluster.lower()

    def get_watermark_model_key(self) -> str:
        return PrestoWatermark.KEY_FORMAT.format(
            database=self.database,
            cluster=self.cluster,
            schema=self.schema,
            table=self.table,
            part_type=self.part_type,
        )

    def get_metadata_model_key(self) -> str:
        return "{database}://{cluster}.{schema}/{table}".format(
            database=self.database,
            cluster=self.cluster,
            schema=self.schema,
            table=self.table,
        )

    def get_col_key(self, col: str) -> str:
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(
            db=self.database,
            cluster=self.cluster,
            schema=self.schema,
            tbl=self.table,
            col=col,
        )
