import abc
from pyhocon import ConfigTree  # noqa: F401
from databuilder import Scoped


class Engine(Scoped):
    """
    A base class that leaves the connection open, allowing for multiple
    different queries on the same Engine. These can be rolled up into
    extractors.
    """

    @abc.abstractmethod
    def init(self, conf: ConfigTree) -> None:
        pass

    @abc.abstractmethod
    def execute(self, query: str):
        """
        Executes query and returns a generator.

        :param query: SQL query to execute.
        """
        pass

    def get_scope(self) -> str:
        return 'engine'
