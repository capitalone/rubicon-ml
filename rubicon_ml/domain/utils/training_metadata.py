from rubicon_ml.exceptions import RubiconException


class TrainingMetadata:
    """A tuple or list of tuples representing metadata
    about a training dataset. Intentionally left arbitrary
    to allow users to use as it suits them.

    Notes
    -----
    `TrainingMetadata` is a simple wrapper around a
    list of tuples for the purpose of validation and
    de/serialization.

    Examples
    --------
    >>> TrainingMetadata([
    >>>     ("s3://bucket/a.parquet", "SELECT * FROM a"),
    >>>     ("s3://bucket/b.parquet", "SELECT * FROM b"),
    >>> ])
    [('s3://bucket/a.parquet', 'SELECT * FROM a'), ...]
    >>> TrainingMetadata(
    >>>     ("s3", ["bucket/a.csv", "bucket/b.csv"], "SELECT * FROM x")
    >>> )
    [('s3', ['bucket/a.csv', 'bucket/b.csv'], 'SELECT * FROM x')]
    """

    def __init__(self, training_metadata):
        if not isinstance(training_metadata, list):
            training_metadata = [training_metadata]

        if not all([isinstance(tm, tuple) for tm in training_metadata]):
            raise RubiconException("`training_metadata` must be a list of tuples.")

        self.training_metadata = training_metadata

    def __repr__(self):
        return str(self.training_metadata)
