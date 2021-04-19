import multiprocessing
import os
from collections import namedtuple

from sklearn.datasets import load_wine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

from rubicon import Rubicon

SklearnTrainingMetadata = namedtuple("SklearnTrainingMetadata", "module_name method")


def run_experiment(project, classifier_cls, wine_datasets, feature_names, **kwargs):
    X_train, X_test, y_train, y_test = wine_datasets

    experiment = project.log_experiment(
        training_metadata=[
            SklearnTrainingMetadata("sklearn.datasets", "load_wine"),
        ],
        model_name=classifier_cls.__name__,
        tags=[classifier_cls.__name__],
    )

    for key, value in kwargs.items():
        experiment.log_parameter(key, value)

    for name in feature_names:
        experiment.log_feature(name)

    classifier = classifier_cls(**kwargs)
    classifier.fit(X_train, y_train)
    classifier.predict(X_test)

    accuracy = classifier.score(X_test, y_test)

    experiment.log_metric("accuracy", accuracy)

    if accuracy >= 0.95:
        experiment.add_tags(["success"])
    else:
        experiment.add_tags(["failure"])


if __name__ == "__main__":
    root_dir = f"{os.path.dirname(os.getcwd())}/rubicon-root"

    rubicon = Rubicon(persistence="filesystem", root_dir=root_dir)
    project = rubicon.get_or_create_project(
        "Concurrent Experiments",
        description="training multiple models in parallel",
    )

    wine = load_wine()
    wine_datasets = train_test_split(
        wine["data"],
        wine["target"],
        test_size=0.25,
    )

    processes = []

    for n_estimators in [10, 20, 30, 40]:
        processes.append(
            multiprocessing.Process(
                target=run_experiment,
                args=[project, RandomForestClassifier, wine_datasets, wine.feature_names],
                kwargs={"n_estimators": n_estimators},
            )
        )

    for n_neighbors in [5, 10, 15, 20]:
        processes.append(
            multiprocessing.Process(
                target=run_experiment,
                args=[project, KNeighborsClassifier, wine_datasets, wine.feature_names],
                kwargs={"n_neighbors": n_neighbors},
            )
        )

    for criterion in ["gini", "entropy"]:
        for splitter in ["best", "random"]:
            processes.append(
                multiprocessing.Process(
                    target=run_experiment,
                    args=[project, DecisionTreeClassifier, wine_datasets, wine.feature_names],
                    kwargs={
                        "criterion": criterion,
                        "splitter": splitter,
                    },
                )
            )

    for process in processes:
        process.start()

    for process in processes:
        process.join()
