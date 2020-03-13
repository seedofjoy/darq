.. darq documentation master file, created by
   sphinx-quickstart on Fri Mar 13 13:24:56 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

darq
====

.. toctree::
   :maxdepth: 2
   :caption: Contents:

|pypi| |license|

Current Version: |release|


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Reference
---------

.. automodule:: darq.app
   :members:

.. automodule:: darq.connections
   :members:

.. automodule:: darq.worker
   :members: func, Retry, Worker

.. automodule:: darq.cron
   :members: cron

.. automodule:: darq.jobs
   :members: JobStatus, Job

.. include:: ../CHANGES.rst

.. |pypi| image:: https://img.shields.io/pypi/v/darq.svg
   :target: https://pypi.python.org/pypi/darq
.. |license| image:: https://img.shields.io/pypi/l/darq.svg
   :target: https://github.com/seedofjoy/darq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _watchgod: https://pypi.org/project/watchgod/
.. _arq: http://python-rq.org/
