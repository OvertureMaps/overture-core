"""Unit tests for the bbox parameter validation pattern."""

import re

import pytest

from overture_core.bbox import BBOX_PARAM_PATTERN


class TestBboxParamPattern:
    """Verify BBOX_PARAM_PATTERN accepts valid bbox params and rejects malformed input."""

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "1,2,3,4",
            "-122.5,37.5,-122.0,38.0",
            "-180,-90,180,90",
            "0.0,0.0,1.5,2.25",
        ],
    )
    def test_accepts_valid(self, value):
        assert re.fullmatch(BBOX_PARAM_PATTERN, value)

    @pytest.mark.parametrize(
        "value",
        [
            "1,2,3",
            "1,2,3,4,5",
            "1,2,3,a",
            "1, 2, 3, 4",
            "1e5,2,3,4",
            ".5,2,3,4",
            "1.,2,3,4",
            "1,2,3,4'",
            '1,2,3,4"',
            "1,2,3,4;DROP TABLE x",
            ",1,2,3",
            "1,2,3,",
        ],
    )
    def test_rejects_invalid(self, value):
        assert not re.fullmatch(BBOX_PARAM_PATTERN, value)
