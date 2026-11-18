import os
import sqlite3

from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json


class SqliteRepository(BaseRepository):
    """The sqlite repository persists Rubicon data to a sqlite database

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon
        data to.
    """

    BINARY_TABLE = "binary_data"
    DOMAIN_TABLE = "domain_objects"

    INIT_QUERY = f"""
CREATE TABLE IF NOT EXISTS {BINARY_TABLE}(
    object_id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    data BLOB
);
CREATE TABLE IF NOT EXISTS {DOMAIN_TABLE}(
    object_id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    data TEXT
)
    """

    def __init__(self, root_dir: str, **storage_options):

        self.db_path = os.path.join(root_dir, "rubicon.sqlite")
        self.con = sqlite3.connect(self.db_path)
        self.con.executescript(self.INIT_QUERY)

    def _persist_bytes(self, bytes_data, bytes_path):

        self.con.execute(f"""
INSERT INTO {self.BINARY_TABLE} (path, data)
VALUES (:path, :data)
ON CONFLICT (path)
DO UPDATE SET data = excluded.data;
        """, {"path": str(bytes_path), "data": bytes_data})



    def _persist_domain(self, domain, path):
        json_domain = json.dumps(domain)
        self.con.execute(f"""
INSERT INTO {self.DOMAIN_TABLE} (path, data)
VALUES (:path, :data)
ON CONFLICT (path)
DO UPDATE SET data = excluded.data;
        """, {"path": str(path), "data": json_domain})
