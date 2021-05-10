# Workflow

Rubicon can be integrated into workflow pipelines, like
[Prefect](https://docs.prefect.io/) via the `workflow` package.

## Setup

To get started, first install and activate the `user-environment`:

```
conda env create -f notebooks/user-environment.yml
conda activate rubicon
```

Then, follow these instructions to get [Prefect setup
locally](https://docs.prefect.io/core/getting_started/installation.html#running-the-local-server-and-ui)
or follow the quick steps below:


* Ensure Docker Desktop is running with your proxy configured if
  necessary:
    * Docker Desktop > Preferences > Resources > Proxies

* Set the Prefect Orchestration to Local and start the server:

    ```
    prefect backend server

    prefect server start
    ```

* In another terminal window, start at least one agent to handle
  incoming flows:

    ```
    prefect agent start
    ```
