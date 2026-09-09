"""Register the bulk operations plugin with Airflow.

Airflow discovers plugins by importing every module in its plugins folder and
looking for AirflowPlugin subclasses. The plugin itself lives in the
ils_middleware package so that it is importable, tested, and type checked
alongside the rest of the code.
"""

from ils_middleware.plugins.bulk_operations import BulkOperationsPlugin  # noqa: F401
