.. _faqs:

FAQs
****

Why the name?
=============

The name comes from a historical context; it's a reference to Caeser
crossing the Rubicon, which nowadays is synonymous with "passing the point of no
return". We chose the name to signify that by using the library, you're making the
decision to provide a repeatable and auditable model development process and
there's no going back on that commitment!

How can I log my data to S3?
============================

Data can be logged either **directly to S3** or to the local filesystem first,
and then **synced with S3**.

**Direct S3 Logging**

Configure the ``Rubicon`` object to log to S3:

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir="s3://my-bucket/path/to/rubicon-root",
    )

If you're logging from your local machine, be sure to 
`configure your AWS credentials <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html>`_.

If you're logging from an EC2, ensure that the IAM role that your EC2 instance
is using has at least ``s3:GetObject``, ``s3:PutObject``, and ``s3:DeleteObject``
actions allowed for your S3 bucket:

.. code-block:: python

    {
        "Sid": "AllowS3Objects",
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
        ],
        "Resource": [
            "arn:aws:s3:::[BUCKET_NAME]",
            "arn:aws:s3:::[BUCKET_NAME]/*"
        ]
    }

**Syncing the Local Filesystem with S3**

Local logging can easily be synced with an S3 bucket using ``Rubicon.sync()``.

.. code-block:: python

    local_rubicon = Rubicon(persistence="filesystem", root_dir="/rubicon-root")
    local_project = local_rubicon.get_project("Sync Example")

    local_rubicon.sync(
        project_name=local_project.name,
        s3_root_dir="s3://my-bucket/path/to/rubicon-root",
    )

This would result in the local "Sync Demo" project being copied to the
specified S3 bucket. Under-the-hood, it uses the AWS CLI ``sync`` method. So,
you'd need to have the AWS CLI installed and ensure your credentials are set.

Why does rubicon-ml offer Prefect integration?
==============================================

`Prefect <https://docs.prefect.io/>`_ is a popular workflow management system
that can be used to create machine learning pipelines. The Prefect
integration makes it easy to drop logging tasks into existing flows.

Why was the dashboard built with Dash?
======================================

We decided to use `dash <https://dash.plotly.com/>`_ as our initial dashboarding
solution for the following reasons:

* low barrier to entry for Python developers
* built in support of the ``plotly`` data visualization library
* can be rendered inside Jupyter Notebooks or JupyterLab with minimal effort
* can be easily deployed to static URL
* compatible with the Python ecosystem (HoloViews)
* compatible with React components

We welcome suggestions to improve the dashboard or even contributions of
additional dashboarding solutions!

Could rubicon-ml be used outside of a machine learning workflow?
================================================================

Yes. The :ref:`terminology<glossary>` was designed for machine learning
workflows, but the library is flexible! An ``experiment`` can simply represent any
unit of work that you'd like to compare multiple runs of. In fact, we've used
``rubicon_ml`` to capture performance benchmarks while developing the library so we
could better evaluate areas of improvement and also have a record of the data
supporting our decisions.


How does rubicon-ml compare to MLFlow?
======================================
At the highest level, rubicon-ml isn't trying to do as much as MLFlow. rubicon-ml is simply a logging library (with some visualizations), while MLFlow is a full model lifecycle management tool. rubicon-ml is designed to be lightweight and not prescribe a full model lifecycle pattern, but instead assist users in enhancing whatever pattern they've already established.

Architecturally, rubicon-ml does not require a hosted server for remote logging like the MLFlow tracking server. rubicon-ml uses `fsspec <https://github.com/fsspec/filesystem_spec>`_ to provide a bring-your-own-backend interface where users can log rubicon-ml data to any arbitrary local or S3 filesystem, as well as directly in-memory for experimentation purposes. If the need arose, it should also be trivial to implement any of the other backends that fsspec supports:

* https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
* https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations

That being said, rubicon-ml's logging capabilities do have some overlap with the MLFlow Tracking part of their service. Largely, both libraries are logging the same information, albeit in different ways.

Like MLFlow, rubicon-ml also aims to offer integrations with commonly used tools, such as `Scikit-learn <https://capitalone.github.io/rubicon-ml/integrations/integration-sklearn.html>`_. Again, rubicon-ml tries to be less prescriptive in these integrations. For example, `MLFlow says exactly what it will log <https://mlflow.org/docs/latest/tracking.html#scikit-learn>`_  when using with Scikit-learn. rubicon-ml has a set of defaults that are logged to each estimator, but also `supports user-defined loggers <https://capitalone.github.io/rubicon-ml/api_reference.html#rubicon_ml.sklearn.RubiconPipeline>`_ for any estimator, like the built-in `FilterEstimatorLogger <https://github.com/capitalone/rubicon-ml/blob/main/rubicon_ml/sklearn/filter_estimator_logger.py>`_.

Is rubicon-ml's dashboard compatible with Docker?
=================================================

The rubicon-ml dashboard is just a `Dash app <https://plotly.com/dash/>`_, so it can be dockerized the same way as any other Dash app. 
You may need to write a small python script to run from the container, rather than the dashboard module or CLI itself, in order to pass necessary parameters to the dashboard's ``dash_kwargs`` or ``run_server_kwargs`` in `serve <https://capitalone.github.io/rubicon-ml/api_reference.html#rubicon_ml.viz.Dashboard.serve>`_.


