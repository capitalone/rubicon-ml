import logging
import warnings

FAILURE_MODE = "raise"
FAILURE_MODES = ["log", "raise", "warn"]


def set_failure_mode(failure_mode):
    global FAILURE_MODE

    if failure_mode not in FAILURE_MODES:
        raise ValueError(f"`failure_mode` must be one of {FAILURE_MODES}")

    FAILURE_MODE = failure_mode


def failsafe(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if FAILURE_MODE == "raise":
                raise e
            elif FAILURE_MODE == "warn":
                warnings.warn(repr(e))
            elif FAILURE_MODE == "log":
                logging.error(repr(e))

    return wrapper
