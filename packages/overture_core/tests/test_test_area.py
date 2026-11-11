"""Unit tests for the test_area contract helpers."""

import pytest

from overture_core.test_area import (
    area_polygon_wkt,
    bbox_polygon_wkt,
    is_wkt_area,
    parse_area_envelope,
    parse_bbox,
    validate_area,
)

ICELAND = "-25,63,-13,67"
ICELAND_ENVELOPE = (-25.0, 63.0, -13.0, 67.0)
ICELAND_TRIANGLE = "POLYGON ((-25 63, -13 63, -25 67, -25 63))"
ICELAND_MULTI = "MULTIPOLYGON (((-25 63, -13 63, -13 67, -25 67, -25 63)))"
# Self-intersecting ring: ST_Intersects can raise a TopologyException on it.
BOWTIE = "POLYGON ((0 0, 4 4, 4 0, 0 4, 0 0))"


class TestIsWktArea:
    @pytest.mark.parametrize(
        "area",
        [
            ICELAND_TRIANGLE,
            ICELAND_MULTI,
            "POLYGON((0 0, 1 0, 1 1, 0 0))",
            "  multipolygon (((0 0, 1 0, 1 1, 0 0)))",
        ],
    )
    def test_wkt(self, area):
        assert is_wkt_area(area)

    @pytest.mark.parametrize(
        "area",
        [
            "",
            ICELAND,
            "POINT (1 1)",
            # A prefix match is not enough: these would only fail later, in
            # the engine that parses the WKT.
            "POLYGONX ((0 0, 1 0, 1 1, 0 0))",
            "MULTIPOLYGONX (((0 0, 1 0, 1 1, 0 0)))",
        ],
    )
    def test_not_wkt(self, area):
        assert not is_wkt_area(area)


class TestParseAreaEnvelope:
    @pytest.mark.parametrize(
        "area", [ICELAND, ICELAND_TRIANGLE, ICELAND_MULTI, f"  {ICELAND_TRIANGLE} "]
    )
    def test_envelope(self, area):
        assert parse_area_envelope(area) == ICELAND_ENVELOPE

    def test_parse_bbox_returns_envelope(self):
        assert parse_bbox(ICELAND) == ICELAND_ENVELOPE

    @pytest.mark.parametrize(
        "area, message",
        [
            ("", "bbox must be"),
            ("-25,63,-13", "bbox must be"),
            ("-13,63,-25,67", "min must be < max"),
            ("POINT (1 1)", "bbox must be"),
            ("POLYGONX ((0 0, 1 0, 1 1, 0 0))", "bbox values must be numbers"),
            (
                "POLYGON ((0 0, 1 0, 1 1, 0 0))'); DROP TABLE x;--",
                "unexpected characters",
            ),
            ("POLYGON ((-25 63, -13 63, -25 67, -25 63)", "could not be parsed"),
            ("POLYGON ((-25 63, -13 63, -25 67))", "could not be parsed"),
            ("POLYGON (EMPTY)", "is empty"),
            (BOWTIE, "not a valid polygon"),
            (
                "MULTIPOLYGON (((0 0, 3 0, 3 3, 0 3, 0 0)), ((2 2, 5 2, 5 5, 2 5, 2 2)))",
                "not a valid polygon",
            ),
            ("POLYGON ((0 0, 200 0, 200 1, 0 0))", "longitudes must be"),
            ("POLYGON ((0 0, 1 0, 1 100, 0 0))", "latitudes must be"),
        ],
    )
    def test_rejects(self, area, message):
        with pytest.raises(ValueError, match=message):
            parse_area_envelope(area)


class TestValidateArea:
    @pytest.mark.parametrize("area", ["", ICELAND, ICELAND_TRIANGLE, ICELAND_MULTI])
    def test_accepts(self, area):
        validate_area(area)

    @pytest.mark.parametrize("area", ["not-a-bbox", BOWTIE])
    def test_rejects(self, area):
        with pytest.raises(ValueError):
            validate_area(area)


class TestPolygonWkt:
    def test_bbox_polygon_wkt(self):
        assert (
            bbox_polygon_wkt(ICELAND)
            == "POLYGON((-25.0 63.0, -13.0 63.0, -13.0 67.0, -25.0 67.0, -25.0 63.0))"
        )

    def test_area_polygon_wkt_passes_wkt_through(self):
        assert area_polygon_wkt(f"  {ICELAND_TRIANGLE} ") == ICELAND_TRIANGLE

    def test_area_polygon_wkt_converts_bbox(self):
        assert area_polygon_wkt(ICELAND) == bbox_polygon_wkt(ICELAND)

    def test_area_polygon_wkt_rejects_invalid_polygon(self):
        with pytest.raises(ValueError, match="not a valid polygon"):
            area_polygon_wkt(BOWTIE)
