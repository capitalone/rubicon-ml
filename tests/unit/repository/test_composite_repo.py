import os
import uuid
from unittest.mock import patch

import fsspec

from rubicon_ml import domain
from rubicon_ml.repository import CompositeRepository, MemoryRepository
from rubicon_ml.repository.utils import slugify


def test_initialization():
    memory_repoA = MemoryRepository()
    memory_repoB = MemoryRepository()
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    assert len(composite_repo.repositories) == 2

    assert composite_repo.repositories[0].PROTOCOL == "memory"
    assert composite_repo.repositories[0].root_dir == "/root"
    assert (
        type(composite_repo.repositories[0].filesystem)
        == fsspec.implementations.memory.MemoryFileSystem
    )

    assert composite_repo.repositories[1].PROTOCOL == "memory"
    assert composite_repo.repositories[1].root_dir == "/root"
    assert (
        type(composite_repo.repositories[1].filesystem)
        == fsspec.implementations.memory.MemoryFileSystem
    )


def test_cat():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    path = root_dir + "/test_cat.txt"
    text = "testing_cat"

    for repository in composite_repo.repositories:
        repository.filesystem.write_text(path, text)

    assert composite_repo._cat(path) == b"testing_cat"


def test_cat_paths():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    path1 = root_dir + "/test_cat_paths1.txt"
    path2 = root_dir + "/test_cat_paths2.txt"
    text = "testing_cat_paths"
    for repository in composite_repo.repositories:
        repository.filesystem.write_text(path1, text)
        repository.filesystem.write_text(path2, text)

    assert composite_repo._cat_paths([path1, path2]) == [b"testing_cat_paths", b"testing_cat_paths"]


def test_exists():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    path = root_dir + "/test_exists.txt"
    text = "testing_exists"

    for repository in composite_repo.repositories:
        repository.filesystem.write_text(path, text)

    assert composite_repo._exists(path)


def test_glob():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    file_names = ["bat.txt", "cat.txt", "hat.txt", "tree.md", "door.md"]
    text = "generic message"

    for repository in composite_repo.repositories:
        for filename in file_names:
            repository.filesystem.write_text(os.path.join(root_dir, filename), text)

    assert composite_repo._glob("*.txt") == file_names[0:3]


def test_ls_directories_only():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    path_directory1 = root_dir + "/example_directory1"
    path_directory2 = root_dir + "/example_directory2"
    for repository in composite_repo.repositories:
        repository.filesystem.makedirs(path_directory1, exist_ok=True)
        repository.filesystem.makedirs(path_directory2, exist_ok=True)
        repository.filesystem.write_text(os.path.join(root_dir, "example.txt"), "example_text")

    assert composite_repo._ls_directories_only(root_dir) == [path_directory1, path_directory2]


def test_ls():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    file_names = ["file1.txt", "file2.txt", "file3.txt"]
    text = "generic message"
    for repository in composite_repo.repositories:
        for filename in file_names:
            repository.filesystem.write_text(os.path.join(root_dir, filename), text)

    assert composite_repo._ls(root_dir) == [
        os.path.join(root_dir, "file1.txt"),
        os.path.join(root_dir, "file2.txt"),
        os.path.join(root_dir, "file3.txt"),
    ]


def test_mkdir():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    directory_path = root_dir + "/example_directory"

    composite_repo._mkdir(directory_path)
    for repository in composite_repo.repositories:
        assert repository.filesystem.exists(directory_path)


def test_modified():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    directory_path = root_dir + "/example_directory"

    composite_repo._mkdir(directory_path)

    modified = []
    for repository in composite_repo.repositories:
        modified.append(repository.filesystem.modified(directory_path))

    assert composite_repo._modified(root_dir) == modified


@patch("fsspec.implementations.memory.MemoryFileSystem.open")
@patch("fsspec.implementations.memory.MemoryFileSystem.mkdirs")
def test_persist_bytes(mock_mkdirs, mock_open):
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    data = b"test data {uuid.uuid4()}"
    bytes_path = root_dir + "/path/to/data"
    for repository in composite_repo.repositories:
        repository._persist_bytes(bytes_data=data, path=bytes_path)

    assert mock_mkdirs.call_count == 2
    assert mock_open.call_count == 2


@patch("fsspec.implementations.memory.MemoryFileSystem.open")
@patch("fsspec.implementations.memory.MemoryFileSystem.mkdirs")
def test_persist_domain(mock_mkdirs, mock_open):
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = f"{root_dir}{slugify(project.name)}/metadata.json"

    for repository in composite_repo.repositories:
        repository._persist_domain(project, project_metadata_path)

    assert mock_mkdirs.call_count == 2
    assert mock_open.call_count == 2


def test_read_bytes():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    data = b"test data {uuid.uuid4()}"
    bytes_path = root_dir + "/path/to/data"

    composite_repo._persist_bytes(bytes_data=data, path=bytes_path)

    assert composite_repo._read_bytes(bytes_path) == data


def test_read_domain():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = f"{root_dir}{slugify(project.name)}/metadata.json"

    composite_repo._persist_domain(project, project_metadata_path)

    assert composite_repo._read_domain(project_metadata_path) == project


def test_rm():
    root_dir = "/root/" + str(uuid.uuid4())
    memory_repoA = MemoryRepository(root_dir)
    memory_repoB = MemoryRepository(root_dir)
    composite_repo = CompositeRepository([memory_repoA, memory_repoB])

    file_names = ["bat.txt", "cat.txt", "hat.txt", "tree.txt", "door.txt"]
    text = "generic message"

    for repository in composite_repo.repositories:
        for filename in file_names:
            with repository.filesystem.open(os.path.join(root_dir, filename)) as f:
                f.write_text(text)

    composite_repo._rm(root_dir)

    assert composite_repo._ls(root_dir) == []
