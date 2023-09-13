import functools
import logging
import traceback
import warnings
from typing import Callable, Optional

FAILURE_MODE = "raise"
FAILURE_MODES = ["log", "raise", "warn"]
TRACEBACK_CHAIN = False
TRACEBACK_LIMIT = None


def set_failure_mode(
    failure_mode: str, traceback_chain: bool = False, traceback_limit: Optional[int] = None
) -> None:
    """Set the failure mode.

    Parameters
    ----------
    failure_mode : str
        The name of the failure mode to set. "raise" to raise all exceptions,
        "log" to catch all exceptions and log them via `logging.error`, "warn"
        to catch all exceptions and re-raise them as warnings via `warnings.warn`.
        Defaults to "raise".
    traceback_chain : bool, optional
        True to display each error in the traceback chain when logging or warning,
        False to display only the first. Defaults to False.
    traceback_limit : int, optional
        The depth of the traceback displayed when logging or warning. 0 to display
        only the error's text, each increment shows another line of the traceback.
    """
    global FAILURE_MODE
    global TRACEBACK_CHAIN
    global TRACEBACK_LIMIT

    if failure_mode not in FAILURE_MODES:
        raise ValueError(f"`failure_mode` must be one of {FAILURE_MODES}")

    FAILURE_MODE = failure_mode
    TRACEBACK_CHAIN = traceback_chain
    TRACEBACK_LIMIT = traceback_limit


def failsafe(func: Callable) -> Callable:
    @functools.wraps(func)
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
