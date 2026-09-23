from __future__ import annotations

import json
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Union

from overture_spark import SparkPlatform, getSparkSedonaSession
from overture_spark.secret_engines import Databricks
from overture_spark.test_area import filter_df_to_area, validate_area

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def pretty_print_elapsed_time(elapsed_ms: float):
    """
    Converts and pretty-prints the elapsed time from milliseconds to
    hours, minutes, seconds, and milliseconds.
    """
    seconds, milliseconds = divmod(elapsed_ms, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"


class MissingParameterError(Exception):
    def __init__(self, param_name):
        super().__init__(f"Missing required parameter: '{param_name}'")
        self.param_name = param_name


class JobResult:
    """
    Base class for storing the results of a job.

    Attributes:
        isSuccess (bool): Indicates if the job was successful.
        messages (list of str): List to store messages logged during the job.
        elapsed (float): Time in milliseconds taken by the job.
        exception (Exception): Exception that was raised during the job, if any.
        params (dict): Parameters with which the job was run.
    """

    def __init__(self):
        self.isSuccess = False
        self.messages = []
        self.elapsed = 0.0
        self.exception = None
        self.exception_traceback = []
        self.params = {}
        self.data = {}

    def log(self, message):
        """
        Adds a message to the result's messages

        Args:
            message (str): The message to log.
        """
        self.messages.append(message)

    def to_json(self) -> Dict:
        """
        Converts the result object to a dict that can be serialized as JSON string.
        """
        return {
            "isSuccess": self.isSuccess,
            "messages": self.messages,
            "elapsed": self.elapsed,
            "exception": str(self.exception) if self.exception else None,
            "exception_traceback": self.exception_traceback,
            "params": self.params,
            "data": self.data,
        }


class SparkSedonaJob(ABC):
    """Cluster-side base class for Overture Spark/Sedona jobs.

    Subclass and implement ``execute_job()``. The platform runner (Glue script,
    Databricks notebook, Wherobots runner — supplied by ``overture-airflow-provider``)
    instantiates the subclass and calls ``run()`` on the executor; ``run()``
    lazily builds the configured Sedona ``SparkSession`` via
    ``init_spark_for_platform()`` (platform autodetected) unless one was injected.

    This class knows nothing about Airflow or job submission — that is the
    provider/factory's responsibility. It only defines how a job executes once it is
    already running on a cluster.
    """

    def __init__(self):
        self.spark_platform = None
        self.spark: SparkSession = None
        self.start: float = 0.0
        self.result = self.get_result()
        self.sql = None
        self.secrets_engine = None

    def init_spark_for_platform(
        self,
        spark_platform: SparkPlatform = None,
        extra_spark_conf: Union[str, Dict[str, str]] = "{}",
    ):
        # Lazy: this is the only place SparkSedonaJob touches pyspark directly,
        # matching getSparkSedonaSession's lazy-import convention.
        from pyspark.sql import SQLContext

        self.spark_platform = spark_platform or SparkPlatform.autodetect()
        if isinstance(extra_spark_conf, str):
            try:
                self.log("Parsing params json")
                extra_spark_conf_dict = json.loads(extra_spark_conf)
            except Exception as parse_exc:
                raise Exception(
                    f"Cannot parse extra_spark_conf, invalid json: [{extra_spark_conf}]"
                ) from parse_exc
        else:
            extra_spark_conf_dict = extra_spark_conf
        self.spark = getSparkSedonaSession(
            spark_platform=self.spark_platform, extra_spark_conf=extra_spark_conf_dict
        )
        self.sql = SQLContext(
            sparkContext=self.spark.sparkContext, sparkSession=self.spark
        )

        return self

    def with_secrets_engine(self, secrets_engine):
        self.secrets_engine = secrets_engine
        return self

    def get_result(self):
        # Default implementation of initializing a result
        # This can be overridden by child classes
        return JobResult()

    def log(self, message):
        """Logs a message into the result object and prints it to stdout."""
        # TODO: where (else) do we want to log these?
        timestamp = datetime.now().isoformat()
        if isinstance(message, dict) or isinstance(message, list):
            print(timestamp)
            print(json.dumps(message, indent=4))
            self.result.messages.append(message)
        else:
            message_w_timestamp = f"[{timestamp}] {message}"
            self.result.messages.append(message_w_timestamp)
            print(message_w_timestamp)

    def log_data(self, key, value, is_secret=False):
        """Adds a key-value pair of information to the result object and also prints it."""
        if not is_secret:
            print(
                f"[{datetime.now().isoformat()}] {key}: {json.dumps(value, indent=4)}"
            )
        self.result.data[key] = value

    def check_output_writable(self, output_path):
        """Verify that the output path is writable before starting processing."""
        output_path = output_path.replace("s3://", "s3a://")
        self.log(f"Checking output path is writable: {output_path}")
        jvm = self.spark._jvm
        uri = jvm.java.net.URI(output_path)
        conf = self.spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        dir_path = jvm.org.apache.hadoop.fs.Path(output_path.rstrip("/"))
        if not fs.exists(dir_path):
            # Mirror Spark's behavior by ensuring the output directory exists.
            # If we cannot create it, treat this as a permissions / writability failure.
            if not fs.mkdirs(dir_path):
                raise PermissionError(f"Cannot create output directory: {output_path}")
        test_file = jvm.org.apache.hadoop.fs.Path(dir_path, "_write_precheck")
        try:
            stream = fs.create(test_file, True)
            try:
                stream.write(0)
            finally:
                stream.close()
        except Exception as exc:
            # Include the underlying exception details to make debugging easier.
            self.log(
                f"Failed to verify output path writability for {output_path}: "
                f"{exc.__class__.__name__}: {exc}"
            )
            raise PermissionError(
                f"Output path is not writable: {output_path}. "
                f"Underlying error: {exc.__class__.__name__}: {exc}"
            ) from exc
        finally:
            try:
                fs.delete(test_file, False)
            except Exception:
                pass

    @abstractmethod
    def execute_job(self):
        """
        Abstract method that defines the main logic of the job which should be implemented by child classes.
        """

    def run(self, params: Union[Dict, str] = "{}") -> JobResult:
        if not self.spark:
            self.init_spark_for_platform()

        try:
            self.log(f"Starting job {self.__class__.__name__}")
            self.start = time.time()

            if isinstance(params, str):
                try:
                    self.log("Parsing params json")
                    self.result.params = json.loads(params)
                except Exception as parse_exc:
                    raise Exception(
                        f"Cannot parse params, invalid json: [{params}]"
                    ) from parse_exc
            else:
                self.result.params = params
            print(json.dumps(self.result.params, indent=4))
            self.log("Executing job...")
            self.execute_job()
            self.result.isSuccess = True
        except Exception as e:
            self.result.exception = e
            self.result.exception_traceback = traceback.format_exc().splitlines()
            self.log("Job failed, see result.exception for details")
        finally:
            end = time.time()
            self.result.elapsed = (end - self.start) * 1000  # Convert to milliseconds
            self.log(f"Elapsed: {pretty_print_elapsed_time(self.result.elapsed)}")

        return self.result

    def get_param(
        self, param_name: str, default_value: str = None, is_required: bool = True
    ) -> str:
        if not self.result or not self.result.params:
            value = default_value
        else:
            value = self.result.params.get(param_name, default_value)

        if isinstance(value, str):
            value = value.strip()
            value = value.replace("s3://", "s3a://")

        if is_required and (value is None or value == ""):
            raise MissingParameterError(param_name)

        return default_value if value is None or value == "" else value

    def get_test_area_param(self) -> str:
        """Optional dev/testing geographic scope.

        Returns the validated 'test_area' param — a bbox
        'min_lon,min_lat,max_lon,max_lat' or a (MULTI)POLYGON WKT — or ""
        for the full planet.
        """
        test_area = self.get_param("test_area", default_value="", is_required=False)
        validate_area(test_area)  # fail fast on a malformed area
        return test_area

    def apply_test_area_filter(
        self,
        df,
        geometry_col: str = "geometry",
        is_wkb: bool = True,
        test_area: str = None,
    ):
        """Filter df to the job's test_area param; no-op when the param is unset.

        Rows with NULL geometry are kept (see overture_spark.test_area docs).
        """
        test_area = self.get_test_area_param() if test_area is None else test_area
        if test_area:
            self.log(
                f"Applying test_area filter {test_area} on column '{geometry_col}'"
            )
        return filter_df_to_area(
            df, test_area, geometry_col=geometry_col, is_wkb=is_wkb
        )

    def get_secret(self, secret_name: str) -> str:
        if self.secrets_engine is None:
            self.secrets_engine = self._default_secrets_engine()
        return self.secrets_engine.get_secret(secret_name)

    def _default_secrets_engine(self):
        # Platform runners are Overture-free and don't inject an engine, so pick one here.
        platform = self.spark_platform or SparkPlatform.autodetect()
        if platform == SparkPlatform.DATABRICKS:
            return Databricks()
        raise RuntimeError(
            f"No secrets engine configured for platform {platform.name}; "
            "call with_secrets_engine() first"
        )
