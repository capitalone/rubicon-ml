import warnings


def log_parameter_with_warning(experiment, name, value):
    try:
        experiment.log_parameter(name=name, value=value)
    except Exception as e:
        warning = (
            f"{str(e)}: "
            f"failed to write parameter '{name}' with value {value} of type {type(value)}. "
            f"try using the `FilterEstimatorLogger` with `ignore=['{name}']`"
        )

        warnings.warn(warning)
