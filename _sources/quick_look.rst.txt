.. _quick-look:

Quick Look
**********

Rubicon has two main components, both powered by open source projects. The first component is the Python library,
which is powered by Intake's `filesystem specification <https://github.com/intake/filesystem_spec>`_
(``fsspec``) for filesystem logging and `Intake <https://github.com/intake/intake>`_ itself
for the collaboration and sharing process. The second component is the dashboard,
powered by `Dash <https://dash.plotly.com/>`_.

Python Library
==============

The library provides an API for storing model inputs, outputs, and other related data and offers a
process for sharing a selected subset of logged data with collaborators and reviewers.

Logging
-------

The logging library supports three types of persistence:

  * **in-memory**: for testing and development - no need to clean up after yourself!
  * **local**: for personal projects and individual exploration
  * **S3**: for collaborating, sharing, and backing up local work

Within Rubicon, the implementations of these filesystems are very lightweight,
with most functionality coming directly from ``fsspec``.  In fact, Rubicon's persistence
layer is designed to be easily extensible and could be extended to support any other type
of persistence that `fsspec supports <https://filesystem-spec.readthedocs.io/en/latest/index.html#implementations>`_.

**Local Logging**

Let's use an 
`example from scikit-learn <https://scikit-learn.org/stable/auto_examples/preprocessing/plot_scaling_importance.html>`_
to show how Rubicon's logging library can be integrated into a basic model. First, load the dataset:

.. code-block:: python

  from sklearn.datasets import load_wine
  from sklearn.model_selection import train_test_split
  
  wine = load_wine()
  X_train, X_test, y_train, y_test = train_test_split(wine.data, wine.target, test_size=0.50)

Then, configure a Rubicon :ref:`project<library-reference-project>`:

.. code-block:: python

  from rubicon import Rubicon
  
  rubicon = Rubicon(persistence="filesystem", root_dir="/rubicon-root")
  project = rubicon.create_project("Wine Classification")

And just like that, you're ready to log to Rubicon! Below we'll create a ``run_experiment``
function to abstract our Rubicon logging into a single spot.

.. code-block:: python

  import numpy as np
  import pandas as pd
  from sklearn import metrics
  from sklearn.decomposition import PCA
  from sklearn.naive_bayes import GaussianNB
  from sklearn.pipeline import make_pipeline
  from sklearn.preprocessing import StandardScaler

  def run_experiment(project, is_standardized=False, n_components=2):
      experiment = project.log_experiment(model_name=GaussianNB.__name__)
          
      experiment.log_parameter("is_standardized", is_standardized)
      experiment.log_parameter("n_components", n_components)

      for name in wine.feature_names:
          experiment.log_feature(name)

      if is_standardized:
          classifier = make_pipeline(
              StandardScaler(), PCA(n_components=n_components), GaussianNB()
          )
      else:
          classifier = make_pipeline(PCA(n_components=n_components), GaussianNB())
                                                          
      classifier.fit(X_train, y_train)
      pred_test = classifier.predict(X_test)
      accuracy = metrics.accuracy_score(y_test, pred_test)
                                                                  
      experiment.log_metric("accuracy", accuracy)
                                                                      
      confusion_matrix = pd.crosstab(
          wine.target_names[y_test], wine.target_names[pred_test],
          rownames=["actual"], colnames=["predicted"],
      )
                                                                                              
      experiment.log_dataframe(
          confusion_matrix, tags=["confusion matrix"]
      )

      if accuracy >= .9:
          experiment.add_tags(["success"])
      else:
          experiment.add_tags(["failure"])

Rubicon logging is threadsafe, so we'll use the ``multiprocessing`` library to run and log
14 unique experiments in parallel:

.. code-block:: python

  import multiprocessing

  processes = []

  for is_standardized in [True, False]:
      for n_components in range(1, 15, 2):
          processes.append(multiprocessing.Process(
              target=run_experiment, args=[project],
              kwargs={"is_standardized": is_standardized, "n_components": n_components}
          ))

  for process in processes:
      process.start()
          
  for process in processes:
      process.join()

**S3 Logging**

Logging to S3 is as simple as changing the ``root_dir`` when instantiating the ``Rubicon``
object.

.. code-block:: python

  rubicon = Rubicon(persistence="filesystem", root_dir="s3://my-bucket/path/to/rubicon-root")

Alternatively, you can use ``Rubicon.sync`` to sync a project with S3 after its been logged locally!

.. code-block:: python

  rubicon = Rubicon(persistence="filesystem", root_dir="/rubicon-root")
  local_project = rubicon.get_project("Wine Classification")

  rubicon.sync(local_project.name, "s3://my-bucket/path/to/rubicon-root")

**Increasing Performance**

Rubicon is lightweight computationally, but reading and writing to S3 takes
time. For this reason, Rubicon
:ref:`exposes an asynchronous client<library-reference-async>` to communicate with S3
without blocking.

As noted above, Rubicon logging is threadsafe so you can use external tools like Dask
and Prefect to speed up your logging.

Collaborating and Sharing
-------------------------

Once you have a Rubicon project stored in S3, anyone with access to that bucket can
use the Python library to pull down your project and explore the data themselves or
they can visualize the project within the dashboard (see below).

Additionally, Rubicon offers a process to share a selected subset of your logged data via
**publishing** and **consuming** custom Intake catalogs.

**Publishing**

First, we'll publish some experiments by generating an Intake catalog. The catalog file (YAML)
simply points to the actual ``rubicon`` data and can be shared and versioned independently of
your rubicon project data.

.. note::
    Publishing does not change or move the data, it simply references it at a certain point in time.

You can use the ``experiment_tags`` parameter to publish experiments with specific tags, like "success":

.. code-block:: python

  rubicon = Rubicon(persistence="filesystem", root_dir="s3://my-bucket/path/to/rubicon-root")
  rubicon.publish(
      project.name, experiment_tags=["success"], output_filepath="/wine-catalog.yml"
  )

The resulting file will have a root ``sources`` key, followed by a number of sources
representing experiments and/or projects that look something like this:

.. code-block:: yaml

  experiment_337ff698_2eef_4c16_b31f_03127e49e01c:
      args:
          experiment_id: 337ff698-2eef-4c16-b31f-03127e49e01c
          project_name: Wine Classification
          urlpath: s3://my-bucket/path/to/rubicon-root
      driver: rubicon_experiment

**Consuming Published Experiments**

If you've been given a catalog of published experiments, you can easily load these
with ``intake`` and the custom ``rubicon.intake_rubicon`` driver, which both come
installed with the library.

.. code-block:: python

  import intake

  catalog = intake.open_catalog("/wine-catalog.yml")

You can use the catalog object to load the published experiments into memory. 

.. note::
    Loading a catalog does not physically copy any data.


Dashboard
=========

Rubicon comes with a UI add-on (installable as ``rubicon[ui]``) that allows you to
explore, visualize, and compare data within your Rubicon projects. If you have
git integration enabled (you should), the dashboard will group your experiments
by commit automatically and link you directly to the corresponding model code.

Use the CLI to launch to the dashboard locally:

.. code-block:: shell

  rubicon ui --root-dir /rubicon-root

.. image:: _static/images/dashboard.png
  :alt: Rubicon dashboard
