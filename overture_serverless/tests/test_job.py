from __future__ import annotations

import pytest

from overture_serverless.job import ServerlessPythonJob


class _EchoJob(ServerlessPythonJob):
    def execute_job(self) -> None:
        self.value = self.get_param("value", default=None, is_required=False)


def test_run_parses_json_params_and_calls_execute_job():
    job = _EchoJob()
    job.run('{"value": "hello"}')
    assert job.value == "hello"


def test_run_accepts_dict_params():
    job = _EchoJob()
    job.run({"value": "hello"})
    assert job.value == "hello"


def test_get_param_returns_default_when_missing():
    job = _EchoJob()
    job.run({})
    assert job.get_param("missing", default="fallback", is_required=False) == "fallback"


def test_get_param_raises_when_required_and_missing():
    job = _EchoJob()
    job._params = {}
    with pytest.raises(ValueError, match="Missing required parameter: value"):
        job.get_param("value")


def test_get_param_strips_whitespace_only_string_to_default():
    job = _EchoJob()
    job._params = {"value": "   "}
    assert job.get_param("value", default="fallback", is_required=False) == "fallback"


def test_log_prints_message(capsys):
    job = _EchoJob()
    job.log("hello from job")
    assert "hello from job" in capsys.readouterr().out
