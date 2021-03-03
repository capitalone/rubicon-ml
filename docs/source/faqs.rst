.. _faqs:

FAQs
****

Why the name Rubicon?
=====================

The name rubicon comes from a historical context; it's a reference to Caeser
crossing the rubicon, which nowadays is synonymous with "passing the point of no
return". We chose the name to signify that by using Rubicon, you're making the
decision to provide a repeatable and auditable model development process and
there's no going back on that commitment!

How can I log my data to S3?
============================

Data can be logged either **directly to S3** or to the local filesystem first,
and then **synced with S3**.

**Direct S3 Logging**

Configure the ``Rubicon`` object to log to S3:

.. code-block:: python

    from rubicon import Rubicon

    rubicon = Rubicon(persistence="filesystem", root_dir="s3://my-bucket/path/to/rubicon-root")

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

We recommend using the :ref:`asynchronous client<library-reference-async>` when logging to S3 directly.

**Syncing the Local Filesystem with S3**

Local logging can easily be synced with an S3 bucket using ``Rubicon.sync()``.

.. code-block:: python

    local_rubicon = Rubicon(persistence="filesystem", root_dir="/rubicon-root")
    local_project = local_rubicon.get_project("Sync Example")

    local_rubicon.sync(
        project_name=local_project.name, s3_root_dir="s3://my-bucket/path/to/rubicon-root"
    )

This would result in the local "Sync Demo" project being copied to the
specified S3 bucket. Under-the-hood, it uses the AWS CLI ``sync`` method. So,
you'd need to have the AWS CLI installed and ensure your credentials are set.

Could Rubicon be used outside of a machine learning workflow?
=============================================================

Yes. Rubicon's :ref:`terminology<glossary>` was designed for machine learning
workflows, but Rubicon is flexible! An ``experiment`` can simply represent any
unit of work that you'd like to compare multiple runs of. In fact, we've used
Rubicon to capture performance benchmarks while developing the library so we
could better evaluate areas of improvement and also have a record of the data
supporting our decisions.

Anything else?
==============

If you have any other questions, open an issue! Maybe you'll see your question
here one day!
