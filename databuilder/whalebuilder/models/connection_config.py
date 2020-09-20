from typing import Optional, List


class ConnectionConfigSchema(object):
    def __init__(
            self,
            type: str,
            host: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            name: Optional[str] = None,
            catalog: Optional[str] = None,
            included_schemas: List = [],
            excluded_schemas: List = [],
            included_keys: Optional[List[str]] = None,
            excluded_keys: Optional[List[str]] = None,
            included_key_regex: Optional[str] = None,
            excluded_key_regex: Optional[str] = None,
            build_script_path: Optional[str] = None,
            venv_path: Optional[str] = None,
            python_binary: Optional[str] = None,
            key_path: Optional[str] = None,
            project_id: Optional[str] = None,
            project_credentials: Optional[str] = None,
            page_size: Optional[str] = None,
            filter_key: Optional[str] = None,
            **kwargs):

        self.host = host
        self.type = type
        self.username = username
        self.password = password
        self.name = name
        self.catalog = catalog
        self.included_schemas = included_schemas
        self.excluded_schemas = excluded_schemas
        self.included_keys = included_keys
        self.excluded_keys = included_keys
        self.included_key_regex = included_key_regex
        self.excluded_key_regex = excluded_key_regex
        self.build_script_path = build_script_path
        self.venv_path = venv_path
        self.python_binary = python_binary
        self.key_path = key_path
        self.project_id = project_id
        self.project_credentials = project_credentials
        self.page_size = page_size
        self.filter_key = filter_key
