from typing import Optional, List


class ConnectionConfigSchema(object):
    def __init__(
        self,
        metadata_source: str,
        dialect: Optional[str] = None,
        uri: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        name: Optional[str] = None,
        database: Optional[str] = None,
        instance: Optional[str] = None,
        cluster: Optional[str] = None,
        is_location_parsing_enabled: bool = False,
        included_schemas: List = [],
        excluded_schemas: List = [],
        included_keys: Optional[List[str]] = None,
        excluded_keys: Optional[List[str]] = None,
        included_key_regex: Optional[str] = None,
        excluded_key_regex: Optional[str] = None,
        included_tables_regex: Optional[str] = None,
        build_script_path: Optional[str] = None,
        venv_path: Optional[str] = None,
        python_binary: Optional[str] = None,
        key_path: Optional[str] = None,
        project_id: Optional[str] = None,
        project_credentials: Optional[str] = None,
        page_size: Optional[str] = None,
        filter_key: Optional[str] = None,
        where_clause_suffix: Optional[str] = "",
        **kwargs,
    ):

        self.uri = uri
        self.port = port
        if metadata_source is not None:
            metadata_source = metadata_source.lower()
        self.metadata_source = metadata_source
        self.dialect = dialect
        self.username = username
        self.password = password
        self.name = name
        self.database = database
        self.instance = instance
        self.cluster = cluster
        self.is_location_parsing_enabled = is_location_parsing_enabled
        self.included_schemas = included_schemas
        self.excluded_schemas = excluded_schemas
        self.included_keys = included_keys
        self.excluded_keys = included_keys
        self.included_key_regex = included_key_regex
        self.excluded_key_regex = excluded_key_regex
        self.included_tables_regex = included_tables_regex
        self.build_script_path = build_script_path
        self.venv_path = venv_path
        self.python_binary = python_binary
        self.key_path = key_path
        self.project_id = project_id
        self.key_path = key_path
        self.project_credentials = project_credentials
        self.page_size = page_size
        self.filter_key = filter_key
        self.where_clause_suffix = where_clause_suffix

        self.infer_conn_string()

    def infer_conn_string(self):
        if self.metadata_source == "bigquery":
            project_id = self.project_id
            conn_string = f"bigquery://{project_id}"
        elif self.metadata_source == "neo4j":
            conn_string = f"bolt://{self.uri}:{self.port}"
        else:
            username_password_placeholder = (
                f"{self.username}:{self.password}" if self.password is not None else ""
            )

            if self.metadata_source in ["redshift"]:
                self.dialect = "postgres"
            elif self.metadata_source == "hivemetastore":
                self.dialect = self.dialect
            else:
                self.dialect = self.metadata_source
            uri = self.uri
            port = self.port
            database = self.database or ""

            conn_string = f"{self.dialect}://{username_password_placeholder}@{uri}:{port}/{database}"

        self.conn_string = conn_string
