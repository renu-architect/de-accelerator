"""Component/integration tests run Spark in local mode inside the pytest process: give the driver JVM enough heap for the wide M3 query
plans (SCD2, Gold). Must be set before the session-scoped `spark` fixture starts the JVM; it only affects test runs."""

import os

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
