import logging
import os
import shutil
import tempfile
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from zipfile import ZipFile

import fsspec

from rubicon_ml.repository.utils import slugify

if TYPE_CHECKING:
    from rubcion_ml.domain import Experiment

LOGGER = logging.Logger(__name__)
logging.captureWarnings(True)


class ArchiveMixin:
    def _archive(
        self,
        project_name: str,
        experiments: Optional[List["Experiment"]] = None,
        remote_rubicon_root: Optional[str] = None,
    ) -> str:
        warnings.warn(
            "Archiving is deprecated. A more robust replacement is coming soon.", DeprecationWarning
        )

        remote_s3 = True if remote_rubicon_root and remote_rubicon_root.startswith("s3") else False
        root_dir = remote_rubicon_root if remote_rubicon_root is not None else self.root_dir

        archive_dir = os.path.join(root_dir, slugify(project_name), "archives")

        ts = datetime.timestamp(datetime.now())
        archive_path = os.path.join(archive_dir, "archive-" + str(ts))
        zip_archive_filename = str(archive_path + ".zip")

        experiments_path = self._get_experiment_metadata_root(project_name)

        if not remote_s3:
            if not self.filesystem.exists(archive_dir):
                self.filesystem.mkdirs(archive_dir, exist_ok=True)

        file_name = None
        with tempfile.NamedTemporaryFile() as tf:
            if experiments is not None:
                with ZipFile(tf, "x") as archive:
                    experiment_paths = []

                    for experiment in experiments:
                        experiment_paths.append(os.path.join(experiments_path, experiment.id))
                    for file_path in experiment_paths:
                        archive.write(file_path, os.path.basename(file_path))

                file_name = archive.filename
            else:
                file_name = shutil.make_archive(tf.name, "zip", experiments_path)

            with fsspec.open(zip_archive_filename, "wb") as fp:
                with open(file_name, "rb") as tf:
                    fp.write(tf.read())

        return zip_archive_filename

    def _experiments_from_archive(
        self,
        project_name: str,
        remote_rubicon_root: str,
        latest_only: Optional[bool] = False,
    ):
        warnings.warn(
            "Archiving is deprecated. A more robust replacement is coming soon.", DeprecationWarning
        )

        root_dir = self.root_dir
        shutil.copy(
            os.path.join(remote_rubicon_root, slugify(project_name), "metadata.json"),
            os.path.join(root_dir, slugify(project_name)),
        )

        archive_dir = os.path.join(remote_rubicon_root, slugify(project_name), "archives")
        if not self.filesystem.exists(archive_dir):
            raise ValueError("`remote_rubicon_root` has no archives.")

        dest_experiments_dir = self._get_experiment_metadata_root(project_name)
        if not self.filesystem.exists(dest_experiments_dir):
            self.filesystem.mkdirs(dest_experiments_dir, exist_ok=True)

        if not latest_only:
            for zip_archive_name in self.filesystem.ls(archive_dir):
                zip_archive_filepath = os.path.join(archive_dir, zip_archive_name)

                with ZipFile(zip_archive_filepath, "r") as curr_archive:
                    curr_archive.extractall(dest_experiments_dir)
        else:
            latest_zip_archive_filepath = None
            latest_time = None

            for zip_archive in self.filesystem.ls(archive_dir):
                zip_archive_filepath = os.path.join(archive_dir, zip_archive)
                mod_time = self.filesystem.modified(zip_archive_filepath)

                if latest_time is None:
                    latest_time = mod_time
                    latest_zip_archive_filepath = zip_archive_filepath
                elif mod_time > latest_time:
                    latest_zip_archive_filepath = zip_archive_filepath
                    latest_time = mod_time
            with ZipFile(latest_zip_archive_filepath, "r") as zip_archive:
                zip_archive.extractall(dest_experiments_dir)
