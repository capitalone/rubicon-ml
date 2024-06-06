import pytest
from dash import Dash

from rubicon_ml.viz import ExperimentsTable


def test_experiments_table(viz_experiments):
    experiments_table = ExperimentsTable(experiments=viz_experiments, is_selectable=True)

    expected_experiment_ids = [e.id for e in viz_experiments]

    for experiment in experiments_table.experiments:
        assert experiment.id in expected_experiment_ids

        expected_experiment_ids.remove(experiment.id)

    assert len(expected_experiment_ids) == 0
    assert experiments_table.is_selectable is True


def test_experiments_table_no_git_commit(viz_experiments):
    for experiment in viz_experiments:
        experiment._domain.commit_hash = ""

    experiments_table = ExperimentsTable(experiments=viz_experiments, is_selectable=True)

    assert len(viz_experiments) == len(experiments_table.experiments)


def test_experiments_table_load_data(viz_experiments):
    experiments_table = ExperimentsTable(experiments=viz_experiments)
    experiments_table.load_experiment_data()

    expected_experiment_ids = [e.id for e in viz_experiments]
    expected_metric_names = [m.name for m in viz_experiments[0].metrics()]
    expected_parameter_names = [p.name for p in viz_experiments[0].parameters()]

    for record in experiments_table.experiment_records:
        assert record["id"] in expected_experiment_ids
        assert all([record.get(name) is not None for name in expected_metric_names])
        assert all([record.get(name) is not None for name in expected_parameter_names])

    assert all(
        [
            name not in experiments_table.hidden_columns
            for name in expected_metric_names + expected_parameter_names
        ]
    )

    assert experiments_table.commit_hash == viz_experiments[0].commit_hash
    assert experiments_table.github_url == f"test.github.url/tree/{viz_experiments[0].commit_hash}"


@pytest.mark.parametrize("filter_by", ["tags", "names"])
def test_experiments_table_load_filtered_data(filter_by, viz_experiments):
    if filter_by == "tags":
        tags = ["a", "b"]
        qtype = "and"

        expected_metric_names = [m.name for m in viz_experiments[0].metrics(tags=tags, qtype=qtype)]
        expected_parameter_names = [
            p.name for p in viz_experiments[0].parameters(tags=tags, qtype=qtype)
        ]

        experiments_table_kwargs = {
            "metric_query_tags": tags,
            "metric_query_type": qtype,
            "parameter_query_tags": tags,
            "parameter_query_type": qtype,
        }
    elif filter_by == "names":
        expected_metric_names = ["test metric 0"]
        expected_parameter_names = ["test param 1", "test param 2"]

        experiments_table_kwargs = {
            "metric_names": expected_metric_names,
            "parameter_names": expected_parameter_names,
        }

    experiments_table = ExperimentsTable(
        experiments=viz_experiments,
        **experiments_table_kwargs,
    )
    experiments_table.load_experiment_data()

    expected_experiment_ids = [e.id for e in viz_experiments]

    all_metric_names = [m.name for m in viz_experiments[0].metrics()]
    all_parameter_names = [p.name for p in viz_experiments[0].parameters()]
    unexpected_metric_names = list(set(all_metric_names).difference(set(expected_metric_names)))
    unexpected_parameter_names = list(
        set(all_parameter_names).difference(set(expected_parameter_names))
    )

    for record in experiments_table.experiment_records:
        assert record["id"] in expected_experiment_ids
        assert all([record.get(name) is not None for name in expected_metric_names])
        assert all([record.get(name) is not None for name in expected_parameter_names])

        # unexpected should still be in table...
        assert all([record.get(name) is not None for name in unexpected_metric_names])
        assert all([record.get(name) is not None for name in unexpected_parameter_names])

    # ...but they should be hidden
    assert all(
        [
            name in experiments_table.hidden_columns
            for name in unexpected_metric_names + unexpected_parameter_names
        ]
    )


def test_experiments_table_layout(viz_experiments):
    experiments_table = ExperimentsTable(experiments=viz_experiments)
    experiments_table.load_experiment_data()
    layout = experiments_table.layout

    assert len(layout.children) == 4
    assert layout.children[-1].children.id == "experiment-table"
    assert layout.children[-2].id == "publish-modal"


def test_experiments_table_layout_not_selectable(viz_experiments):
    experiments_table = ExperimentsTable(experiments=viz_experiments, is_selectable=False)
    experiments_table.load_experiment_data()
    layout = experiments_table.layout

    assert len(layout.children) == 3
    assert layout.children[-1].children.id == "experiment-table"
    assert layout.children[-2].id == "publish-modal"


def test_experiments_table_register_callbacks(viz_experiments):
    experiments_table = ExperimentsTable(experiments=viz_experiments)
    experiments_table.app = Dash(__name__, title="test callbacks")
    experiments_table.register_callbacks()

    callback_values = list(experiments_table.app.callback_map.values())

    assert len(callback_values) == 4

    registered_callback_names = [callback["callback"].__name__ for callback in callback_values]

    assert "update_selected_column_checkboxes" in registered_callback_names
    assert "update_hidden_experiment_table_cols" in registered_callback_names
    assert "update_selected_experiment_table_rows" in registered_callback_names
    assert "toggle_publish_modal" in registered_callback_names
