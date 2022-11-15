import logging
import traceback
import warnings

FAILURE_MODE = "raise"
FAILURE_MODES = ["log", "raise", "warn"]
TRACEBACK_CHAIN = False
TRACEBACK_LIMIT = None


def set_failure_mode(failure_mode, traceback_chain=False, traceback_limit=None):
    global FAILURE_MODE
    global TRACEBACK_CHAIN
    global TRACEBACK_LIMIT

    if failure_mode not in FAILURE_MODES:
        raise ValueError(f"`failure_mode` must be one of {FAILURE_MODES}")

    FAILURE_MODE = failure_mode
    TRACEBACK_CHAIN = traceback_chain
    TRACEBACK_LIMIT = traceback_limit


def failsafe(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if FAILURE_MODE == "raise":
                raise e
            elif FAILURE_MODE == "warn":
                warnings.warn(traceback.format_exc(limit=TRACEBACK_LIMIT, chain=TRACEBACK_CHAIN))
            elif FAILURE_MODE == "log":
                logging.error(traceback.format_exc(limit=TRACEBACK_LIMIT, chain=TRACEBACK_CHAIN))

    return wrapper
