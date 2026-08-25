"""Lightweight base class for a portable "serverless job" contract.

Subclass and implement ``execute_job()``; a runner (container, script, cron
job, whatever) instantiates the subclass and calls ``run()``.

Plain Python by design — no cloud SDKs, no framework dependencies. The same
subclass runs identically on any backend or on a developer laptop for local
iteration.
"""

import json
from abc import ABC, abstractmethod


class ServerlessPythonJob(ABC):
    def __init__(self) -> None:
        self._params: dict = {}

    def run(self, params: "str | dict" = "{}") -> None:
        print(f"Starting job {self.__class__.__name__}")
        self._params = json.loads(params) if isinstance(params, str) else params
        if not isinstance(self._params, dict):
            raise TypeError(
                f"params must decode to a JSON object/dict, got {type(self._params).__name__}"
            )
        print(f"Received params: {sorted(self._params.keys())}")
        self.execute_job()
        print(f"Job {self.__class__.__name__} completed successfully")

    def get_param(self, name: str, default=None, is_required: bool = True):
        value = self._params.get(name, default)
        if isinstance(value, str):
            value = value.strip() or None
        if is_required and value is None:
            raise ValueError(f"Missing required parameter: {name}")
        return default if value is None else value

    def log(self, msg: str) -> None:
        print(msg)

    @abstractmethod
    def execute_job(self) -> None: ...
