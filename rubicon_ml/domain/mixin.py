class TagMixin:
    """Adds tagging support to a domain model."""

    def add_tags(self, tags):
        self.tags = list(set(self.tags).union(set(tags)))

    def remove_tags(self, tags):
        self.tags = list(set(self.tags).difference(set(tags)))
