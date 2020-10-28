import os
import yaml
from whale.utils import paths


def read_connections():
    if os.path.exists(paths.CONNECTION_PATH):
        with open(paths.CONNECTION_PATH, "r") as f:
            raw_connection_dicts = list(yaml.safe_load_all(f))
    else:
        raw_connection_dicts = []
    return raw_connection_dicts


def get_connection(warehouse_name=None):
    """
    Returns the first connection that has a matching "name" field to `warehouse_name`.
    If "None" is given, the first warehouse in the list is returned.
    """
    connection_dict = {}
    raw_connection_dicts = read_connections()

    if warehouse_name is not None:
        for raw_connection_dict in raw_connection_dicts:
            if "name" in raw_connection_dict:
                if raw_connection_dict["name"] == warehouse_name:
                    connection_dict = raw_connection_dict
        if connection_dict:
            return connection_dict
        else:
            raise Exception(
                f"Warehouse `{warehouse_name}` not found in ~/.whale/config/connections.yaml."
            )
    else:
        if raw_connection_dicts:
            return raw_connection_dicts[0]
        else:
            return {}
