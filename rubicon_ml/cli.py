import click

from rubicon_ml.ui import Dashboard


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
    help="The absolute path to the top level folder holding the Rubicon project(s).",
    required=True,
)
@click.option(
    "--page-size",
    "-ps",
    type=click.IntRange(min=1),
    help="The number of rows that will be displayed on a page within the experiment table.",
    default=10,
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
def ui(root_dir, host, port, debug, page_size, storage_options):
    """Launch the Rubicon Dashboard."""
    # convert the additional storage options into a dict
    # coming in as: ('--key1', 'one', '--key2', 'two')
    storage_options_dict = {
        storage_options[i][2:]: storage_options[i + 1] for i in range(0, len(storage_options), 2)
    }
    dashboard = Dashboard("filesystem", root_dir, page_size=page_size, **storage_options_dict)

    server_kwargs = dict(debug=debug, port=port, host=host)
    server_kwargs = {k: v for k, v in server_kwargs.items() if v is not None}
    dashboard.run_server(**server_kwargs)


# CLI groups

if __name__ == "__main__":
    cli()
