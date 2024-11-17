.. sghi-etl-commons documentation master file, created by sphinx-quickstart on
   Thu Mar 14 20:57:37 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: images/sghi_logo.webp
   :align: center

SGHI ETL Commons
================

This project is part of `SGHI ETL <sghi-etl-core_>`_ projects. Specifically,
this is a collection of common implementations of the interfaces defined by
the `sghi-etl-core`_ project as well as other useful utilities.

Installation
------------

We recommend using the latest version of Python. Python 3.11 and newer is
supported. We also recommend using a `virtual environment`_ in order
to isolate your project dependencies from other projects and the system.

Install the latest sghi-etl-commons version using pip:

.. code-block:: bash

    pip install sghi-etl-commons


API Reference
-------------

.. autosummary::
   :template: module.rst
   :toctree: api
   :caption: API
   :recursive:

     sghi.etl.commons.processors
     sghi.etl.commons.sinks
     sghi.etl.commons.sources
     sghi.etl.commons.utils
     sghi.etl.commons.workflow_builder
     sghi.etl.commons.workflow_definitions


.. _sghi-etl-core: https://github.com/savannahghi/sghi-etl-core/
.. _virtual environment: https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments
