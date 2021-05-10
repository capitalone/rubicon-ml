def _check_for_ui_extras():
    try:
        import dash  # noqa F401
        import dash_html_components as html  # noqa F401
    except ImportError:
        install_command = "pip install rubicon[ui]"
        message = f"Install the packages required for the UI with `{install_command}`."

        raise ImportError(message)


_check_for_ui_extras()

from rubicon_ml.ui.dashboard import Dashboard  # noqa E402

__all__ = ["Dashboard"]
