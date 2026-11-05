"""Guards against runaway Spark query plans.

A plan that grows with the number of unions, joins or ``withColumn`` calls can
kill the driver: Spark renders the whole plan as one string for the UI and
event log, and past the JVM's ~2 GB array limit the driver dies with an OOM.
Data size is irrelevant; only plan shape matters.

``plan_size`` measures the analyzed plan's string length, and
``assert_plan_size`` turns that into a unit-test guard so a package catches
plan growth before prod. Measuring renders the plan string on the driver, so
call these in tests or opt-in checks, never on every action.
"""

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Spark ends a string cut by spark.sql.maxPlanStringLength with this suffix.
_TRUNCATION_SUFFIX = re.compile(r"\.\.\. (\d+) more characters$")


# Default threshold for opt-in runtime checks: far above normal plans (well
# under 1M chars), far below the size where the driver dies.
DEFAULT_MAX_PLAN_CHARS = 10_000_000


class PlanTooLargeError(AssertionError):
    """A DataFrame's plan string exceeds the allowed size."""

    def __init__(self, size: int, max_chars: int):
        super().__init__(
            f"Spark plan is {size:,} chars, over the limit of {max_chars:,}. "
            "The plan likely grows with the number of unions, joins or "
            "withColumn calls; restructure it (e.g. union once, select columns "
            "in one pass, or checkpoint) instead of raising the limit."
        )
        self.size = size
        self.max_chars = max_chars


def plan_size(df: "DataFrame") -> int:
    """Length in characters of the DataFrame's analyzed plan string.

    When ``spark.sql.maxPlanStringLength`` truncates the string, Spark ends it
    with ``... N more characters``; the true size is recovered from that, so the
    result does not depend on the configured cap.
    """
    text = df._jdf.queryExecution().analyzed().toString()
    match = _TRUNCATION_SUFFIX.search(text[-64:])
    if match is None:
        return len(text)
    return len(text) - len(match.group(0)) + int(match.group(1))


def assert_plan_size(df: "DataFrame", max_chars: int) -> int:
    """Return the plan size, raising ``PlanTooLargeError`` if over ``max_chars``."""
    size = plan_size(df)
    if size > max_chars:
        raise PlanTooLargeError(size, max_chars)
    return size
