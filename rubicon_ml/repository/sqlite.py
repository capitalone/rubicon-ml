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


    def __init__(self, root_dir: str, **storage_options):

        self.db_path = os.path.join(root_dir, "rubicon.db")
        self.con = sqlite3.connect(self.db_path)
