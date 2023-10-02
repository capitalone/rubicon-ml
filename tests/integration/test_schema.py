import pytest
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier, XGBRegressor
from xgboost.dask import DaskXGBClassifier, DaskXGBRegressor

PANDAS_SCHEMA_CLS = [RandomForestClassifier, XGBClassifier, XGBRegressor]
DASK_SCHEMA_CLS = [DaskXGBClassifier, DaskXGBRegressor]


def _fit_and_log(X, y, schema_cls, rubicon_project):
    model = schema_cls()
    model.fit(X, y)

    experiment = rubicon_project.log_with_schema(model)


@pytest.mark.integration
@pytest.mark.parametrize("schema_cls", PANDAS_SCHEMA_CLS)
def test_estimator_schema_fit_array(
    schema_cls, make_classification_array, rubicon_project
):
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
