"""Unit tests for UUID generation helpers."""

import uuid as uuid_module
from unittest.mock import MagicMock, patch
from uuid import UUID

from overture_core.uuids import (
    generate_uuid3,
    generate_uuid4,
    generate_uuid5,
    generate_uuid6,
    generate_uuid7,
    generate_uuid8,
)

NAMESPACE_DNS = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class TestGenerateUuid3:
    """Verify generate_uuid3 produces deterministic name-based (MD5) UUIDs."""

    def test_is_deterministic(self):
        assert generate_uuid3(NAMESPACE_DNS, "example.com") == generate_uuid3(
            NAMESPACE_DNS, "example.com"
        )

    def test_matches_known_value(self):
        assert (
            generate_uuid3(NAMESPACE_DNS, "example.com")
            == "9073926b-929f-31c2-abc9-fad77ae3e8eb"
        )

    def test_different_names_differ(self):
        assert generate_uuid3(NAMESPACE_DNS, "a") != generate_uuid3(NAMESPACE_DNS, "b")

    def test_different_namespaces_differ(self):
        other_namespace = UUID("3d6a33ba-1abe-4aaa-abcd-5aa7ccb6ca42")
        assert generate_uuid3(NAMESPACE_DNS, "example.com") != generate_uuid3(
            other_namespace, "example.com"
        )

    def test_returns_str(self):
        assert isinstance(generate_uuid3(NAMESPACE_DNS, "example.com"), str)


class TestGenerateUuid4:
    """Verify generate_uuid4 produces random UUIDs as strings."""

    def test_returns_str(self):
        assert isinstance(generate_uuid4(), str)

    def test_is_valid_uuid(self):
        assert UUID(generate_uuid4()).version == 4

    def test_calls_differ(self):
        assert generate_uuid4() != generate_uuid4()


class TestGenerateUuid5:
    """Verify generate_uuid5 produces deterministic name-based (SHA-1) UUIDs."""

    def test_is_deterministic(self):
        assert generate_uuid5(NAMESPACE_DNS, "example.com") == generate_uuid5(
            NAMESPACE_DNS, "example.com"
        )

    def test_matches_known_value(self):
        assert (
            generate_uuid5(NAMESPACE_DNS, "example.com")
            == "cfbff0d1-9375-5685-968c-48ce8b15ae17"
        )

    def test_different_names_differ(self):
        assert generate_uuid5(NAMESPACE_DNS, "a") != generate_uuid5(NAMESPACE_DNS, "b")

    def test_different_namespaces_differ(self):
        other_namespace = UUID("3d6a33ba-1abe-4aaa-abcd-5aa7ccb6ca42")
        assert generate_uuid5(NAMESPACE_DNS, "example.com") != generate_uuid5(
            other_namespace, "example.com"
        )

    def test_returns_str(self):
        assert isinstance(generate_uuid5(NAMESPACE_DNS, "example.com"), str)

    def test_uuid3_and_uuid5_differ(self):
        assert generate_uuid3(NAMESPACE_DNS, "example.com") != generate_uuid5(
            NAMESPACE_DNS, "example.com"
        )


class TestGenerateUuid6789Fallback:
    """generate_uuid6/7/8 wrap stdlib generators only present on Python 3.14+.

    These tests don't assume which interpreter runs them: each stdlib
    generator is patched onto the `uuid` module (created if absent) to
    exercise the success path, and removed to exercise the
    NotImplementedError fallback, regardless of the actual Python version.
    """

    def test_generate_uuid6_delegates_to_stdlib_when_available(self):
        fake_uuid6 = MagicMock(return_value=UUID(int=1))
        with patch.object(uuid_module, "uuid6", fake_uuid6, create=True):
            result = generate_uuid6(node=0x1234, clock_seq=0x56)
        assert result == str(UUID(int=1))
        fake_uuid6.assert_called_once_with(node=0x1234, clock_seq=0x56)

    def test_generate_uuid6_raises_when_unavailable(self):
        with patch.object(uuid_module, "uuid6", None, create=True):
            try:
                generate_uuid6()
            except NotImplementedError as exc:
                assert "generate_uuid6 requires Python 3.14+" in str(exc)
            else:
                raise AssertionError("expected NotImplementedError")

    def test_generate_uuid7_delegates_to_stdlib_when_available(self):
        fake_uuid7 = MagicMock(return_value=UUID(int=2))
        with patch.object(uuid_module, "uuid7", fake_uuid7, create=True):
            result = generate_uuid7()
        assert result == str(UUID(int=2))
        fake_uuid7.assert_called_once_with()

    def test_generate_uuid7_raises_when_unavailable(self):
        with patch.object(uuid_module, "uuid7", None, create=True):
            try:
                generate_uuid7()
            except NotImplementedError as exc:
                assert "generate_uuid7 requires Python 3.14+" in str(exc)
            else:
                raise AssertionError("expected NotImplementedError")

    def test_generate_uuid8_delegates_to_stdlib_when_available(self):
        fake_uuid8 = MagicMock(return_value=UUID(int=3))
        with patch.object(uuid_module, "uuid8", fake_uuid8, create=True):
            result = generate_uuid8(a=1, b=2, c=3)
        assert result == str(UUID(int=3))
        fake_uuid8.assert_called_once_with(1, 2, 3)

    def test_generate_uuid8_raises_when_unavailable(self):
        with patch.object(uuid_module, "uuid8", None, create=True):
            try:
                generate_uuid8()
            except NotImplementedError as exc:
                assert "generate_uuid8 requires Python 3.14+" in str(exc)
            else:
                raise AssertionError("expected NotImplementedError")
