import h2o
import pandas as pd
import pytest
from h2o import H2OFrame
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.estimators.generic import H2OGenericEstimator
from h2o.estimators.glm import H2OGeneralizedLinearEstimator
from h2o.estimators.random_forest import H2ORandomForestEstimator
from h2o.estimators.targetencoder import H2OTargetEncoderEstimator
from h2o.estimators.xgboost import H2OXGBoostEstimator
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier, XGBRegressor
from xgboost.dask import DaskXGBClassifier, DaskXGBRegressor

from rubicon_ml.schema import registry

PANDAS_SCHEMA_CLS = [
    LGBMClassifier,
    LGBMRegressor,
    RandomForestClassifier,
    XGBClassifier,
    XGBRegressor,
]
DASK_SCHEMA_CLS = [DaskXGBClassifier, DaskXGBRegressor]
H2O_SCHEMA_CLS = [
    H2OGeneralizedLinearEstimator,
    H2OGradientBoostingEstimator,
    H2ORandomForestEstimator,
    H2OTargetEncoderEstimator,
]

h2o.init()

if H2OXGBoostEstimator.available():
    H2O_SCHEMA_CLS.append(H2OXGBoostEstimator)


def _fit_and_log(X, y, schema_cls, rubicon_project):
    model = schema_cls()
    model.fit(X, y)

    rubicon_project.log_with_schema(model)


def _train_and_log(X, y, schema_cls, rubicon_project, schema=None):
    target_name = "target"
    training_frame = pd.concat([X, pd.Series(y)], axis=1)
    training_frame.columns = [*X.columns, target_name]
    training_frame_h2o = H2OFrame(training_frame)

    if schema:
        rubicon_project.set_schema(schema)

    model = schema_cls()
    model.train(
        training_frame=training_frame_h2o,
        x=list(X.columns),
        y=target_name,
    )

    return rubicon_project.log_with_schema(model)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", PANDAS_SCHEMA_CLS)
def test_estimator_schema_fit_array(schema_cls, make_classification_array, rubicon_project):
    X, y = make_classification_array

    _fit_and_log(X, y, schema_cls, rubicon_project)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", PANDAS_SCHEMA_CLS)
def test_estimator_schema_fit_df(schema_cls, make_classification_df, rubicon_project):
    X, y = make_classification_df

    _fit_and_log(X, y, schema_cls, rubicon_project)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", DASK_SCHEMA_CLS)
def test_estimator_schema_fit_dask_array(
    schema_cls,
    make_classification_dask_array,
    rubicon_project,
    dask_client,
):
    X_da, y_da = make_classification_dask_array

    _fit_and_log(X_da, y_da, schema_cls, rubicon_project)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", DASK_SCHEMA_CLS)
def test_estimator_schema_fit_dask_df(
    schema_cls, make_classification_dask_df, rubicon_project, dask_client
):
    X_df, y_da = make_classification_dask_df

    _fit_and_log(X_df, y_da, schema_cls, rubicon_project)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", H2O_SCHEMA_CLS)
@pytest.mark.parametrize("extended_schema", [True, False])
def test_estimator_h2o_schema_train(
    schema_cls,
    extended_schema,
    make_classification_df,
    rubicon_local_filesystem_client_with_project,
):
    _, project = rubicon_local_filesystem_client_with_project

    X, y = make_classification_df
    y = y > y.mean()

    # H2OTargetEncoderEstimator does not support MOJO
    if not extended_schema or schema_cls == H2OTargetEncoderEstimator:
        use_mojo = False
        deserialize_method = "h2o_binary"
        artifact_name = schema_cls.__name__
    else:
        use_mojo = True
        deserialize_method = "h2o_mojo"
        artifact_name = H2OGenericEstimator.__name__

    if extended_schema:
        schema = {
            "name": f"h2o__{schema_cls.__name__}__ext",
            "extends": f"h2o__{schema_cls.__name__}",
            "artifacts": [
                {
                    "self": "log_h2o_model",
                    "artifact_name": artifact_name,
                    "export_cross_validation_predictions": True,
                    "use_mojo": use_mojo,
                },
            ],
        }
    else:
        schema = None

    experiment = _train_and_log(X, y, schema_cls, project, schema)
    model_artifact = experiment.artifact(name=artifact_name)

    if extended_schema:
        # Make sure the extended schema parameters are set properly with the schema from registry
        assert len(registry.get_schema(f"h2o__{schema_cls.__name__}")["parameters"]) == len(
            experiment.parameters()
        )
    else:
        assert len(project.schema_["parameters"]) == len(experiment.parameters())
    assert (
        model_artifact.get_data(deserialize=deserialize_method).__class__.__name__ == artifact_name
    )
