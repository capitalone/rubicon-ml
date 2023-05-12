import os
import warnings

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json


class CompositeRepository(BaseRepository):
    def __init__(self, repositories):
        self.repositories = repositories

    # --- Filesystem Helpers ---

    def _cat(self, path):
        """Returns the contents of the file at `path`."""
        for repo in self.repositories:
            try:
                result = repo._cat(path)
            except Exception as err:
                last_err = err
                pass
            else:
                return result
        raise RubiconException(last_err)

    def _cat_paths(self, metadata_paths):
        """Cat `metadata_paths` to get the list of files to include.
        Ignore FileNotFoundErrors to avoid misc file errors, like hidden
        dotfiles.
        """
        for repo in self.repositories:
            try:
                files = []
                for path, metadata in repo.filesystem.cat(
                    metadata_paths, on_error="return"
                ).items():
                    if isinstance(metadata, FileNotFoundError):
                        warning = f"{path} not found. Was this file unintentionally created?"
                        warnings.warn(warning)
                    else:
                        files.append(metadata)
            except Exception as err:
                last_err = err
                pass
            else:
                return files
        raise RubiconException(last_err)

    def _exists(self, path):
        """Returns True if a file exists at `path`, False otherwise."""
        for repo in self.repositories:
            try:
                exists = repo.filesystem.exists(path)
            except Exception as err:
                last_err = err
                pass
            else:
                return exists
        raise RubiconException(last_err)

    def _glob(self, globstring):
        """Returns the names of the files matching `globstring`."""
        for repo in self.repositories:
            try:
                glob = repo.filesystem.glob(globstring, detail=True)
            except Exception as err:
                last_err = err
                pass
            else:
                return glob
        raise RubiconException(last_err)

    def _ls_directories_only(self, path):
        """Returns the names of all the directories at path `path`."""
        for repo in self.repositories:
            try:
                directories = [
                    os.path.join(p.get("name"), "metadata.json")
                    for p in repo.filesystem.ls(path, detail=True)
                    if p.get("type", p.get("StorageClass")).lower() == "directory"
                ]
            except Exception as err:
                last_err = err
                pass
            else:
                return directories
        raise RubiconException(last_err)

    def _ls(self, path):
        for repo in self.repositories:
            try:
                ls = repo.filesystem.ls(path)
            except Exception as err:
                last_err = err
                pass
            else:
                return ls
        raise RubiconException(last_err)

    def _mkdir(self, dirpath):
        """Creates a directory `dirpath` with parents."""
        res = []
        for repo in self.repositories:
            res.append(repo.filesystem.mkdir(dirpath, parents=True, exist_ok=True))
        return res

    def _modified(self, path):
        res = []
        for repo in self.repositories:
            res.append(repo.filesystem.modified(path))
        return res

    def _persist_bytes(self, bytes_data, path):
        """Write bytes to the filesystem.

        To be implemented by extensions of the base filesystem.
        """
        for repo in self.repositories:
            repo._persist_bytes(bytes_data, path)

    def _persist_domain(self, domain, path):
        """Write a domain object to the filesystem.

        To be implemented by extensions of the base filesystem.
        """
        for repo in self.repositories:
            repo._persist_domain(domain, path)

    def _read_bytes(self, path, err_msg=None):
        """Read bytes from the file at `path`."""
        for repo in self.repositories:
            try:
                open_file = repo.filesystem.open(path, "rb")
            except Exception as err:
                last_err = err
                pass
            else:
                open_file.read()
        raise RubiconException(last_err)

    def _read_domain(self, path, err_msg=None):
        """Read a domain object from the file at `path`."""
        for repo in self.repositories:
            try:
                open_file = repo.filesystem.open(path)
            except Exception as err:
                last_err = err
                pass
            else:
                return json.load(open_file)
        raise RubiconException(last_err)

    def _rm(self, path):
        """Recursively remove all files at `path`."""
        res = []
        for repo in self.repositories:
            res.append(repo.filesystem.rm(path, recursive=True))
        return res
