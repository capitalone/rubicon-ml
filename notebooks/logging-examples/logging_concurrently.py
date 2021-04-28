from collections import namedtuple

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
