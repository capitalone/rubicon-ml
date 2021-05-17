class Base:
    """The base object for all top-level client objects.

    Parameters
    ----------
    domain : one of rubicon.domain.*
        The top-level object's domain instance.
    config : rubicon.client.Config, optional
        The config, which injects the repository to use.
    """

    def __init__(self, domain, config=None):
        self._config = config
        self._domain = domain

    def __str__(self):
        return self._domain.__str__()

    @property
    def repository(self):
        return self._config.repository
