from typing import Dict, Iterable, List, Optional, Union
from whale.models.column_metadata import ColumnMetadata

class TableColumnStats:
    """
    Table stats model.
    """

    LABEL = "Stat"
    KEY_FORMAT = "{db}://{cluster}.{schema}" ".{table}/{col}/{stat_name}/"
    STAT_Column_RELATION_TYPE = "STAT_OF"
    Column_STAT_RELATION_TYPE = "STAT"

    def __init__(
        self,
        table_name: str,
        col_name: str,
        stat_name: str,
        stat_val: str,
        start_epoch: str,
        end_epoch: str,
        db: str = "hive",
        cluster: str = "gold",
        schema: str = None,
    ):
        if schema is None:
            self.schema, self.table = table_name.split(".")
        else:
            self.table = table_name.lower()
            self.schema = schema.lower()
        self.db = db
        self.col_name = col_name.lower()
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.cluster = cluster
        self.stat_name = stat_name
        self.stat_val = stat_val

    def get_table_stat_model_key(self):
        # type: (...) -> str
        return TableColumnStats.KEY_FORMAT.format(
            db=self.db,
            cluster=self.cluster,
            schema=self.schema,
            table=self.table,
            col=self.col_name,
            stat_name=self.stat_name,
        )

    def get_col_key(self):
        # type: (...) -> str
        # no cluster, schema info from the input
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(
            db=self.db,
            cluster=self.cluster,
            schema=self.schema,
            tbl=self.table,
            col=self.col_name,
        )
