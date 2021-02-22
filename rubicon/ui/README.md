## Rubicon Dashboard

The Rubicon Dashboard (powered by [Dash](https://dash.plotly.com/introduction)) offers a way to visualize your logged data within a local or shareable UI. To view the Dashboard, follow these steps:

* install the required dependencies:

    ```
    pip install rubicon[ui]
    ```

* use the CLI to launch the dashboard:

    ```bash
    # usage
    rubicon ui --help

    # run
    rubicon ui --root-dir "/path/to/root" --project-name "Example"
    ```

* view the dashboard at `http://127.0.0.1:8050/`
