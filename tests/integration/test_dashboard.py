import pytest


@pytest.mark.dashboard_test
def test_default_layout(dashboard_setup, dash_duo):
    dash_duo.start_server(dashboard_setup._app)

    assert dash_duo.find_element("#header")
    assert dash_duo.find_element("#title").text == "Rubicon"
    assert dash_duo.find_element("#project-selection")
    assert dash_duo.find_element(".project-selection--label").text == "Projects"
    assert dash_duo.find_element("#project-selection--refresh-projects-btn")

    # experiments view is empty by default until a project is selected
    assert dash_duo.find_element("#empty-view")

    assert dash_duo.find_element("#footer")


@pytest.mark.dashboard_test
def test_refresh_projects(dashboard_setup, dash_duo):
    dash_duo.start_server(dashboard_setup._app)

    # in headless mode, the widths were different and the refresh
    # btn could not be clicked
    dash_duo.driver.set_window_size(1200, 877)

    project_selection_list = dash_duo.find_element("#project-selection--list")
    project_options = project_selection_list.find_elements_by_class_name(
        "project-selection--project"
    )

    assert len(project_options) == 1
    assert project_options[0].text == "Test Project"

    rubicon = dashboard_setup.rubicon_model._rubicon
    project = rubicon.create_project("Second Project")

    dash_duo.find_element("#project-selection--refresh-projects-btn").click()

    project_options = project_selection_list.find_elements_by_class_name(
        "project-selection--project"
    )
    assert len(project_options) == 2
    assert project_options[0].text == project.name


@pytest.mark.dashboard_test
def test_dashboard_project_view_without_metrics_and_parameters(
    dashboard_setup_without_parameters_or_metrics, dash_duo
):
    dash_duo.start_server(dashboard_setup_without_parameters_or_metrics._app)

    project_selection_list = dash_duo.find_element("#project-selection--list")
    project_options = project_selection_list.find_elements_by_class_name(
        "project-selection--project"
    )
    first_project = project_options[0]
    first_project.click()

    dash_duo.wait_for_element("#grouped-project-explorer", timeout=3)

    # check the experiments view
    group_project_explorer = dash_duo.find_element("#grouped-project-explorer")
    assert group_project_explorer

    experiment_details_header = group_project_explorer.find_element_by_id(
        "experiment-deatils-header"
    )
    assert experiment_details_header.text == "Test Project"

    # the project is seeded with 1 different groupings based on commit
    assert len(group_project_explorer.find_elements_by_class_name("group-preview-row")) == 1


@pytest.mark.dashboard_test
def test_select_project_view(dashboard_setup, dash_duo):
    dash_duo.start_server(dashboard_setup._app)

    project_selection_list = dash_duo.find_element("#project-selection--list")
    project_options = project_selection_list.find_elements_by_class_name(
        "project-selection--project"
    )
    first_project = project_options[0]
    first_project.click()

    dash_duo.wait_for_element("#grouped-project-explorer", timeout=3)

    # check the experiments view
    group_project_explorer = dash_duo.find_element("#grouped-project-explorer")
    assert group_project_explorer

    experiment_details_header = group_project_explorer.find_element_by_id(
        "experiment-deatils-header"
    )
    assert experiment_details_header.text == "Test Project"

    # the project is seeded with 4 different groupings based on commit
    assert len(group_project_explorer.find_elements_by_class_name("group-preview-row")) == 4


@pytest.mark.dashboard_test
def test_individual_project_explorer_view(dashboard_setup, dash_duo):
    dash_duo.start_server(dashboard_setup._app)

    project_selection_list = dash_duo.find_element("#project-selection--list")
    project_options = project_selection_list.find_elements_by_class_name(
        "project-selection--project"
    )
    first_project = project_options[0]
    first_project.click()

    dash_duo.wait_for_element("#grouped-project-explorer", timeout=3)

    group_project_explorer = dash_duo.find_element("#grouped-project-explorer")
    first_group_preview_row = group_project_explorer.find_elements_by_class_name(
        "group-preview-row"
    )[0]
    show_btn = first_group_preview_row.find_element_by_class_name(
        "show-group-detail-collapsable-btn"
    )
    show_btn.click()

    # the group detail card holds the individual experiment view
    group_detail_view = dash_duo.find_element(".group-detail-card")
    assert group_detail_view

    action_btn_group = group_detail_view.find_element_by_class_name("btn-group")
    assert action_btn_group
    assert len(action_btn_group.find_elements_by_class_name("btn-progressive")) == 2

    assert group_detail_view.find_element_by_class_name("dash-table-container")
    assert group_detail_view.find_element_by_class_name("experiment-comparison-header")
    assert group_detail_view.find_element_by_class_name("anchor-dropdown")
    assert group_detail_view.find_element_by_class_name("experiment-comparison-plot")

    # click the select all btn
    action_btn_group = group_detail_view.find_element_by_class_name("btn-group")
    action_btn_group.find_elements_by_class_name("btn-progressive")[0].click()

    # check that the plot is visible
    assert group_detail_view.find_element_by_class_name("dash-graph")
