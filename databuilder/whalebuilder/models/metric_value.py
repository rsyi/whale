class MetricValue(object):
    """
    Generic stat object.
    """

    def __init__(
            self,
            database: str,
            cluster: str,
            schema: str,
            table: str,
            execution_time: str,
            name: str,
            value: str,
            description: str = None,
            markdown_blob: str = None,
            is_global: bool = False,
            ):
        self.execution_time = execution_time
        self.description = description
        self.name = name
        self.value = value
        self.is_global = is_global
        self.markdown_blob = markdown_blob
