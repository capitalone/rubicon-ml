import pickle
import uuid

import fsspec
import pandas as pd

from rubicon_ml.repository.v2 import MemoryRepositoryV2 as MemoryRepository


def test_initialization():
    memory_repo = MemoryRepository()

    assert memory_repo.PROTOCOL == "memory"
    assert memory_repo.root_dir == "/root"
    assert isinstance(memory_repo.filesystem, fsspec.implementations.memory.MemoryFileSystem)


def test_persist_dataframe():
    path = f"/root/project-name/dataframes/{uuid.uuid4()}/data"
    df = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])

    memory_repo = MemoryRepository()
    memory_repo._persist_dataframe(df, path)

    with memory_repo.filesystem.open(path, "rb") as f:
        assert df.equals(pickle.load(f))


def test_read_dataframe():
    path = f"/root/project-name/dataframes/{uuid.uuid4()}/data"
    df = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])

    memory_repo = MemoryRepository()

    with memory_repo.filesystem.open(path, "wb") as f:
        pickle.dump(df, f)

    assert df.equals(memory_repo._read_dataframe(path))
