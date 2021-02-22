def test_str(project_client):
    project = project_client

    assert project.__str__() == project._domain.__str__()


def test_get_repository(rubicon_client):
    rubicon = rubicon_client

    assert rubicon.repository == rubicon.config.repository
