import warnings

warnings.warn(
    "The `rubicon_ml.workflow` module is deprecated and will be removed in an upcoming release. "
    "`rubicon_ml` can still be leveraged within custom tasks "
    "(see https://capitalone.github.io/rubicon-ml/integrations/integration-prefect-workflows.html).",
    DeprecationWarning,
)
