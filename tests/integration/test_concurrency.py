import multiprocessing

import pandas as pd
from dask import dataframe as dd

from rubicon.domain.utils import uuid


def _log_all_to_experiment(experiment):
    ddf = dd.from_pandas(pd.DataFrame([0, 1], columns=["a"]), npartitions=1)
    multi_index_df = pd.DataFrame([[0,1,'a'],[1,1,'b'],[2,2,'c'],[3,2,'d']], columns=['a', 'b', 'c'])
    multi_index_df = multi_index_df.set_index(['b', 'a'])

    for _ in range(0, 4):
        experiment.log_metric(uuid.uuid4(), 0)
        experiment.log_feature(uuid.uuid4())
        experiment.log_parameter(uuid.uuid4(), 1)
        experiment.log_artifact(data_bytes=b"artifact bytes", name=uuid.uuid4())
        experiment.log_dataframe(ddf)
        experiment.log_dataframe(multi_index_df)
        experiment.add_tags([uuid.uuid4()])


def _read_all_from_experiment(experiment):
    for _ in range(0, 4):
        experiment.metrics()
        experiment.features()
        experiment.parameters()
        experiment.artifacts()
        experiment.dataframes()
        experiment.tags


def test_filesystem_concurrency(rubicon_local_filesystem_client):
    rubicon = rubicon_local_filesystem_client
    project = rubicon.create_project("Test Concurrency")
    experiment = project.log_experiment()

    processes = []
    for i in range(0, 4):
        process = multiprocessing.Process(target=_read_all_from_experiment, args=[experiment])
        process.start()

        processes.append(process)

    for i in range(0, 4):
        process = multiprocessing.Process(target=_log_all_to_experiment, args=[experiment])
        process.start()

        processes.append(process)

    for process in processes:
        process.join()

    assert len(experiment.metrics()) == 16
    assert len(experiment.features()) == 16
    assert len(experiment.parameters()) == 16
    assert len(experiment.artifacts()) == 16
    assert len(experiment.dataframes()) == 32
    assert len(experiment.tags) == 16
