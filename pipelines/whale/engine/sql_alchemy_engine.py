import importlib
from pyhocon import ConfigFactory, ConfigTree
from sqlalchemy import create_engine
from typing import Iterator

from whale.engine.base_engine import Engine


class SQLAlchemyEngine(Engine):

    """
    An engine that connects via SQLAlchemy.
    """

    CONN_STRING_KEY = "conn_string"
    MODEL_CLASS_KEY = "model_class"
    CREDENTIALS_PATH_KEY = "credentials_path"

    def init(self, conf: ConfigTree):
        """
        Establish connection, import data model class (if provided)

        :param conf: configuration file.
        """
        self.conn_string = conf.get_string(SQLAlchemyEngine.CONN_STRING_KEY)
        self.credentials_path = conf.get(SQLAlchemyEngine.CREDENTIALS_PATH_KEY, None)
        self.connection = self._get_connection()

        model_class = conf.get(SQLAlchemyEngine.MODEL_CLASS_KEY, None)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

    def _get_connection(self):
        """
        Create a SQLAlchemy connection to `conn_string`.
        """
        if self.credentials_path is not None:
            engine = create_engine(
                self.conn_string, credentials_path=self.credentials_path
            )
        else:
            engine = create_engine(self.conn_string)
        conn = engine.connect()
        return conn

    def execute(
        self, query: str, is_dict_return_enabled: bool = False, has_header: bool = False
    ) -> Iterator:
        """
        Execute `query` over `conn_string`, and yield rows.

        :param query: SQL query to execute.
        """
        try:
            results = self.connection.execute(query)
            keys = results.keys()

            if has_header:
                yield keys

            if hasattr(self, "model_class"):
                for row in results:
                    yield self.model_class(**row)
            else:
                for row in results:
                    if is_dict_return_enabled:
                        yield dict(zip(keys, row))
                    else:
                        yield row
        except Exception as e:
            raise e

    def get_scope(self):
        # type: () -> str
        return "engine.sqlalchemy"
