"""Unit tests for the bbox parameter validation helpers."""

import re

import jinja2
import pytest

from overture_core.bbox import (
    BBOX_PARAM_PATTERN,
    BBOX_WKT_JINJA,
    bbox_wkt_jinja,
    validate_bbox,
)


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


class TestValidateBbox:
    """Verify validate_bbox enforces the semantic contract."""

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "1,2,3,4",
            "-122.5,37.5,-122.0,38.0",
            "-180,-90,180,90",
        ],
    )
    def test_accepts_valid(self, value):
        validate_bbox(value)

    @pytest.mark.parametrize(
        ("value", "match"),
        [
            ("1,2,3", "must be 'min_lon,min_lat,max_lon,max_lat'"),
            ("1,2,3,4,5", "must be 'min_lon,min_lat,max_lon,max_lat'"),
            ("1,2,3,a", "values must be numbers"),
            ("181,2,183,4", r"longitudes must be in \[-180, 180\]"),
            ("1,-91,3,4", r"latitudes must be in \[-90, 90\]"),
            ("3,2,1,4", "min must be < max"),
            ("1,4,3,2", "min must be < max"),
            ("1,2,1,4", "min must be < max"),
        ],
    )
    def test_rejects_invalid(self, value, match):
        with pytest.raises(ValueError, match=match):
            validate_bbox(value)


class TestBboxWktJinja:
    """Verify BBOX_WKT_JINJA renders a closed POLYGON WKT literal."""

    @staticmethod
    def render(bbox):
        return jinja2.Template(BBOX_WKT_JINJA).render(params={"bbox": bbox})

    def test_renders_closed_polygon(self):
        assert (
            self.render("1,2,3,4")
            == "POLYGON((1.0 2.0, 3.0 2.0, 3.0 4.0, 1.0 4.0, 1.0 2.0))"
        )

    def test_float_filter_neutralizes_non_numeric_input(self):
        rendered = self.render("1'); DROP TABLE x;--,2,3,4")
        assert rendered == "POLYGON((0.0 2.0, 3.0 2.0, 3.0 4.0, 0.0 4.0, 0.0 2.0))"

    def test_custom_context_expression(self):
        template = bbox_wkt_jinja("dag_run.conf['bbox']")
        rendered = jinja2.Template(template).render(
            dag_run={"conf": {"bbox": "1,2,3,4"}}
        )
        assert rendered == "POLYGON((1.0 2.0, 3.0 2.0, 3.0 4.0, 1.0 4.0, 1.0 2.0))"
