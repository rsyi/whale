import json
import logging

from collections import namedtuple, OrderedDict

from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from whale.extractor.base_spanner_extractor import BaseSpannerExtractor
from whale.models.table_metadata import TableMetadata, ColumnMetadata

from ddlparse import (
    DdlParseBase,
    DdlParseTable,
    re,
    CaselessKeyword,
    Forward,
    Word,
    Regex,
    alphanums,
    delimitedList,
    Suppress,
    Optional,
    Group,
    OneOrMore,
)

LOGGER = logging.getLogger(__name__)


class DdlParse(DdlParseBase):
    """DDL parser"""

    _LPAR, _RPAR, _COMMA, _SEMICOLON, _DOT, _DOUBLEQUOTE, _BACKQUOTE, _SPACE = map(
        Suppress, '(),;."` '
    )
    (
        _CREATE,
        _TABLE,
        _TEMP,
        _CONSTRAINT,
        _NOT_NULL,
        _PRIMARY_KEY,
        _UNIQUE,
        _UNIQUE_KEY,
        _FOREIGN_KEY,
        _REFERENCES,
        _KEY,
        _CHAR_SEMANTICS,
        _BYTE_SEMANTICS,
    ) = map(
        CaselessKeyword,
        "CREATE, TABLE, TEMP, CONSTRAINT, NOT NULL, PRIMARY KEY, UNIQUE, UNIQUE KEY, FOREIGN KEY, REFERENCES, KEY, CHAR, BYTE".replace(
            ", ", ","
        ).split(
            ","
        ),
    )
    _TYPE_UNSIGNED, _TYPE_ZEROFILL = map(
        CaselessKeyword, "UNSIGNED, ZEROFILL".replace(", ", ",").split(",")
    )
    _COL_ATTR_DISTKEY, _COL_ATTR_SORTKEY, _COL_ATTR_CHARACTER_SET = map(
        CaselessKeyword, "DISTKEY, SORTKEY, CHARACTER SET".replace(", ", ",").split(",")
    )
    _FK_MATCH = CaselessKeyword("MATCH") + Word(alphanums + "_")
    (
        _FK_ON,
        _FK_ON_OPT_RESTRICT,
        _FK_ON_OPT_CASCADE,
        _FK_ON_OPT_SET_NULL,
        _FK_ON_OPT_NO_ACTION,
    ) = map(
        CaselessKeyword,
        "ON, RESTRICT, CASCADE, SET NULL, NO ACTION".replace(", ", ",").split(","),
    )
    _FK_ON_DELETE = (
        _FK_ON
        + CaselessKeyword("DELETE")
        + (
            _FK_ON_OPT_RESTRICT
            | _FK_ON_OPT_CASCADE
            | _FK_ON_OPT_SET_NULL
            | _FK_ON_OPT_NO_ACTION
        )
    )
    _FK_ON_UPDATE = (
        _FK_ON
        + CaselessKeyword("UPDATE")
        + (
            _FK_ON_OPT_RESTRICT
            | _FK_ON_OPT_CASCADE
            | _FK_ON_OPT_SET_NULL
            | _FK_ON_OPT_NO_ACTION
        )
    )
    _SUPPRESS_QUOTE = _BACKQUOTE | _DOUBLEQUOTE

    _COMMENT = Suppress("--" + Regex(r".+"))

    _CREATE_TABLE_STATEMENT = (
        Suppress(_CREATE)
        + Optional(_TEMP)("temp")
        + Suppress(_TABLE)
        + Optional(Suppress(CaselessKeyword("IF NOT EXISTS")))
        + Optional(_SUPPRESS_QUOTE)
        + Optional(
            Word(alphanums + "_")("schema")
            + Optional(_SUPPRESS_QUOTE)
            + _DOT
            + Optional(_SUPPRESS_QUOTE)
        )
        + Word(alphanums + "_<>")("table")
        + Optional(_SUPPRESS_QUOTE)
        + _LPAR
        + delimitedList(
            OneOrMore(
                _COMMENT
                |
                # Ignore Index
                Suppress(_KEY + Word(alphanums + "_'`() "))
                | Group(
                    Optional(
                        Suppress(_CONSTRAINT)
                        + Optional(_SUPPRESS_QUOTE)
                        + Word(alphanums + "_")("name")
                        + Optional(_SUPPRESS_QUOTE)
                    )
                    + (
                        (
                            (_PRIMARY_KEY ^ _UNIQUE ^ _UNIQUE_KEY ^ _NOT_NULL)("type")
                            + Optional(_SUPPRESS_QUOTE)
                            + Optional(Word(alphanums + "_"))("name")
                            + Optional(_SUPPRESS_QUOTE)
                            + _LPAR
                            + Group(
                                delimitedList(
                                    Optional(_SUPPRESS_QUOTE)
                                    + Word(alphanums + "_")
                                    + Optional(_SUPPRESS_QUOTE)
                                )
                            )("constraint_columns")
                            + _RPAR
                        )
                        | (
                            (_FOREIGN_KEY)("type")
                            + _LPAR
                            + Group(
                                delimitedList(
                                    Optional(_SUPPRESS_QUOTE)
                                    + Word(alphanums + "_")
                                    + Optional(_SUPPRESS_QUOTE)
                                )
                            )("constraint_columns")
                            + _RPAR
                            + Optional(
                                Suppress(_REFERENCES)
                                + Optional(_SUPPRESS_QUOTE)
                                + Word(alphanums + "_")("references_table")
                                + Optional(_SUPPRESS_QUOTE)
                                + _LPAR
                                + Group(
                                    delimitedList(
                                        Optional(_SUPPRESS_QUOTE)
                                        + Word(alphanums + "_")
                                        + Optional(_SUPPRESS_QUOTE)
                                    )
                                )("references_columns")
                                + _RPAR
                                + Optional(_FK_MATCH)("references_fk_match")  # MySQL
                                + Optional(_FK_ON_DELETE)(
                                    "references_fk_on_delete"
                                )  # MySQL
                                + Optional(_FK_ON_UPDATE)(
                                    "references_fk_on_update"
                                )  # MySQL
                            )
                        )
                    )
                )("constraint")
                | Group(
                    (
                        (
                            _SUPPRESS_QUOTE
                            + Word(alphanums + " _")("name")
                            + _SUPPRESS_QUOTE
                        )
                        ^ (
                            Optional(_SUPPRESS_QUOTE)
                            + Word(alphanums + "_")("name")
                            + Optional(_SUPPRESS_QUOTE)
                        )
                    )
                    + Group(
                        Group(
                            Word(alphanums + "_")
                            + Optional(
                                CaselessKeyword("WITHOUT TIME ZONE")
                                ^ CaselessKeyword("WITH TIME ZONE")
                                ^ CaselessKeyword("PRECISION")
                                ^ CaselessKeyword("VARYING")
                            )
                        )("type_name")
                        + Optional(
                            _LPAR
                            + Regex(r"[\d\*]+\s*,*\s*\d*")("length")
                            + Optional(_CHAR_SEMANTICS | _BYTE_SEMANTICS)("semantics")
                            + _RPAR
                        )
                        + Optional(_TYPE_UNSIGNED)("unsigned")
                        + Optional(_TYPE_ZEROFILL)("zerofill")
                    )("type")
                    + Optional(Regex(r"\<(.*?)\>"))("array_brackets")
                    + Optional(
                        Regex(r"(?!--)", re.IGNORECASE)
                        + Group(
                            Optional(Regex(r"\b(?:NOT\s+)NULL?\b", re.IGNORECASE))(
                                "null"
                            )
                            & Optional(Regex(r"\bAUTO_INCREMENT\b", re.IGNORECASE))(
                                "auto_increment"
                            )
                            & Optional(
                                Regex(r"\b(UNIQUE|PRIMARY)(?:\s+KEY)?\b", re.IGNORECASE)
                            )("key")
                            & Optional(
                                Regex(
                                    r"\bDEFAULT\b\s+(?:((?:[A-Za-z0-9_\.\'\" -\{\}]|[^\x01-\x7E])*\:\:(?:character varying)?[A-Za-z0-9\[\]]+)|(?:\')((?:\\\'|[^\']|,)+)(?:\')|(?:\")((?:\\\"|[^\"]|,)+)(?:\")|([^,\s]+))",
                                    re.IGNORECASE,
                                )
                            )("default")
                            & Optional(
                                Regex(
                                    r"\bCOMMENT\b\s+(\'(\\\'|[^\']|,)+\'|\"(\\\"|[^\"]|,)+\"|[^,\s]+)",
                                    re.IGNORECASE,
                                )
                            )("comment")
                            & Optional(
                                Regex(r"\bENCODE\s+[A-Za-z0-9]+\b", re.IGNORECASE)
                            )(
                                "encode"
                            )  # Redshift
                            & Optional(_COL_ATTR_DISTKEY)("distkey")  # Redshift
                            & Optional(_COL_ATTR_SORTKEY)("sortkey")  # Redshift
                            & Optional(
                                Suppress(_COL_ATTR_CHARACTER_SET)
                                + Word(alphanums + "_")("character_set")
                            )  # MySQL
                        )("constraint")
                    )
                )("column")
                | _COMMENT
            )
        )("columns")
    )

    _DDL_PARSE_EXPR = Forward()
    _DDL_PARSE_EXPR << OneOrMore(_COMMENT | _CREATE_TABLE_STATEMENT)

    def __init__(self, ddl=None, source_database=None):
        super().__init__(source_database)
        self._ddl = ddl
        self._table = DdlParseTable(source_database)

    @property
    def source_database(self):
        """
        Source database option

        :param source_database: enum DdlParse.DATABASE
        """
        return super().source_database

    @source_database.setter
    def source_database(self, source_database):
        super(self.__class__, self.__class__).source_database.__set__(
            self, source_database
        )
        self._table.source_database = source_database

    @property
    def ddl(self):
        """DDL script"""
        return self._ddl

    @ddl.setter
    def ddl(self, ddl):
        self._ddl = ddl

    def parse(self, ddl=None, source_database=None):
        """
        Parse DDL script.

        :param ddl: DDL script
        :return: DdlParseTable, Parsed table define info.
        """

        if ddl is not None:
            self._ddl = ddl

        if source_database is not None:
            self.source_database = source_database

        if self._ddl is None:
            raise ValueError("DDL is not specified")

        ret = self._DDL_PARSE_EXPR.parseString(self._ddl)

        if "schema" in ret:
            self._table.schema = ret["schema"]

        self._table.name = ret["table"]
        self._table.is_temp = True if "temp" in ret else False

        for ret_col in ret["columns"]:

            if ret_col.getName() == "column":
                # add column
                col = self._table.columns.append(
                    column_name=ret_col["name"],
                    data_type_array=ret_col["type"],
                    array_brackets=ret_col["array_brackets"]
                    if "array_brackets" in ret_col
                    else None,
                    constraint=ret_col["constraint"]
                    if "constraint" in ret_col
                    else None,
                )

            elif ret_col.getName() == "constraint":
                # set column constraint
                for col_name in ret_col["constraint_columns"]:
                    col = self._table.columns[col_name]

                    if ret_col["type"] == "PRIMARY KEY":
                        col.not_null = True
                        col.primary_key = True
                    elif ret_col["type"] in ["UNIQUE", "UNIQUE KEY"]:
                        col.unique = True
                    elif ret_col["type"] == "NOT NULL":
                        col.not_null = True

        return self._table


def format_spanner_fields(column_name, column_contents):
    col = OrderedDict()
    col["name"] = column_name
    col["type"] = type = column_contents._data_type
    col["mode"] = column_contents.bigquery_mode
    return json.loads(json.dumps(col, ensure_ascii=False))


class SpannerMetadataExtractor(BaseSpannerExtractor):

    """A metadata extractor for spanner databases, taking the schema metadata
    from the google cloud spanner APIs. This extractor goes through all
    visible instances in the project identified by project_id and iterates over
    all databases it finds. A separate account is configurable through the
    key_path parameter, which should point to a valid json file corresponding
    to a service account.
    """

    def init(self, conf: ConfigTree) -> None:
        BaseSpannerExtractor.init(self, conf)

    def _retrieve_databases(self, instance) -> Any:
        for page in self._page_database_list_results(instance):
            if "databases" not in page:
                continue

            for database in page["databases"]:
                database_name = database["name"]

                database = (
                    self.spanner_service.projects()
                    .instances()
                    .databases()
                    .getDdl(
                        database=database_name,
                    )
                    .execute(num_retries=SpannerMetadataExtractor.NUM_RETRIES)
                )

                if "statements" not in database:
                    continue

                for statement in database["statements"]:
                    # DdlParse currently cannot handle MAX
                    ddl = statement.replace("(MAX)", "(10485760)")

                    if "CREATE TABLE" not in ddl:
                        continue

                    # Using DdlParse to convert Spanner DDL to BQ JSON schema
                    try:
                        parsed_ddl = DdlParse().parse(ddl).columns
                    except:
                        continue

                    cols = []
                    total_cols = 0
                    for column_name, column_contents in parsed_ddl.items():
                        column = format_spanner_fields(column_name, column_contents)
                        total_cols = self._iterate_over_cols(
                            "", column, cols, total_cols + 1
                        )

                    schema = "{}.{}".format(
                        database_name.split("/")[3], database_name.split("/")[5]
                    )

                    table_meta = TableMetadata(
                        database="spanner",
                        cluster=database_name.split("/")[1],
                        schema=schema,
                        name=ddl.split()[2],
                        description=None,
                        columns=cols,
                    )

                    yield (table_meta)

    def _iterate_over_cols(
        self, parent: str, column: dict, cols: List[ColumnMetadata], total_cols: int
    ) -> int:
        if len(parent) > 0:
            col_name = "{parent}.{field}".format(parent=parent, field=column["name"])
        else:
            col_name = column["name"]

        if column["type"] == "RECORD":
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
            )
            cols.append(col)
            total_cols += 1
            for field in column["fields"]:
                total_cols = self._iterate_over_cols(col_name, field, cols, total_cols)
            return total_cols
        else:
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
            )
            cols.append(col)
            return total_cols + 1

    def get_scope(self):
        # type: () -> str
        return "extractor.spanner_table_metadata"
