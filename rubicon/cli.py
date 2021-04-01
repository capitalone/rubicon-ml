import click

from rubicon.ui import Dashboard


@click.group()
@click.version_option()
def cli():
    pass


# Top level CLI commands


@cli.command()
@click.option(
    "--root-dir",
    type=click.STRING,
    help="The absolute path to the top level folder holding the Rubicon project(s).",
    required=True,
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
@click.option(
    "--page-size",
    "-ps",
    type=click.INT,
    help="The number of rows that will be displayed on a page within the experiment table.",
    default=10,
)
def ui(root_dir, host, port, debug, page_size):
    """Launch the Rubicon Dashboard."""
    dashboard = Dashboard("filesystem", root_dir, page_size=page_size)

    server_kwargs = dict(debug=debug, port=port, host=host)
    server_kwargs = {k: v for k, v in server_kwargs.items() if v is not None}
    dashboard.run_server(**server_kwargs)


# CLI groups

if __name__ == "__main__":
    cli()
