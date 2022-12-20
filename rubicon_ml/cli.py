import os
import pprint
import sys
import warnings

import click

from rubicon_ml import Rubicon
from rubicon_ml.client.rubicon_json import RubiconJSON
from rubicon_ml.viz import Dashboard


@click.group()
@click.version_option()
def cli():
    pass


# Top level CLI commands


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
    )
)
@click.option(
    "--root-dir",
    type=click.STRING,
    help="The absolute path to the top level folder holding the rubicon-ml project.",
    required=True,
)
@click.option(  # DEPRECATED
    "--page-size",
    "-ps",
    type=click.IntRange(min=1),
    help="The number of rows that will be displayed on a page within the experiment table.",
    default=10,
)
@click.option(
    "--project-name",
    "-pn",
    type=click.STRING,
    help="The name of the rubicon-ml project to visualize.",
    default=None,
)
@click.option(
    "--host", "-h", type=click.STRING, help="The host to serve the dashboard on.", default=None
)
@click.option(
    "--port", "-p", type=click.STRING, help="The port to serve the dashboard on.", default=None
)
@click.option(
    "--debug", "-d", type=click.BOOL, help="Whether or not to run in debug mode.", default=False
)
@click.argument("storage_options", nargs=-1, type=click.UNPROCESSED)
def ui(root_dir, page_size, project_name, host, port, debug, storage_options):
    """Launch the Rubicon Dashboard."""
    if page_size != 10:
        warnings.warn("The `--page-size` option will be deprecated in a future release.")

    # convert the additional storage options into a dict
    # coming in as: ('--key1', 'one', '--key2', 'two')
    cleaned_storage_options = {
        storage_options[i][2:]: storage_options[i + 1] for i in range(0, len(storage_options), 2)
    }
    rubicon = Rubicon(persistence="filesystem", root_dir=root_dir, **cleaned_storage_options)

    if project_name is None:
        warnings.warn(
            "`--project-name` will be a required option in a future release. "
            "Visualizing the most recently logged project."
        )

        project_name = [p.name for p in rubicon.projects()][0]

    project = rubicon.get_project(name=project_name)
    dashboard = Dashboard(project.experiments())

    run_server_kwargs = dict(debug=debug, port=port, host=host)
    run_server_kwargs = {k: v for k, v in run_server_kwargs.items() if v is not None}
    dashboard.serve(run_server_kwargs=run_server_kwargs)


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
    )
)
@click.option(
    "--root-dir",
    type=click.STRING,
    default=os.environ.get("RUBICON_ROOT_DIR"),
    help="The absolute path to the top level folder holding the rubicon-ml project.",
    required=False,
)
@click.option(
    "--project-name",
    default=os.environ.get("RUBICON_PROJECT_NAME"),
    type=click.STRING,
    help="Name of rubicon project to retrieve experiments from",
    required=False,
)
@click.option(
    "--color",
    "-c",
    type=click.STRING,
    help="Color of query output",
    required=False,
)
@click.option(
    "--bf",
    is_flag=True,
    show_default=True,
    default=True,
    required=False,
    help="Flag that switches pretty print output format to basic format",
)
@click.argument(
    "query",
    nargs=1,
    required=True,
)
def search(root_dir, project_name, color, bf, query):
    """
    This function provides capability to query  rubicon experiments from the command line without using python
    directly and output the JSON in the command line.
    """

    if project_name is None or root_dir is None:
        click.secho("No --root-dir or --project-name provided. Exiting...", fg="red")
        sys.exit(1)

    rubicon = Rubicon(persistence="filesystem", root_dir=root_dir)
    try:
        project = rubicon.get_project(name=project_name)
    except Exception as e:
        click.secho(e, fg="red")
        sys.exit(1)

    rb_json = RubiconJSON(projects=project)
    res = rb_json.search(query)
    if not isinstance(res, list):
        res = [res]
    matches = [r.value for r in res]
    if "color" in click.get_current_context().params:
        fg_color = color

    result = matches if bf else pprint.pformat(matches, indent=1)

    click.secho(result, fg=fg_color)


# CLI groups
if __name__ == "__main__":
    cli()
