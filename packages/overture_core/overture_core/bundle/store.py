"""Storage backends for the bundle model.

`overture_core.bundle.bundle` does all its object storage access through a
`BundleStore`:

- `S3BundleStore` -- the default, backed by `overture_core.cloud.aws.object`.
- `InMemoryBundleStore` -- dict-backed, for tests and offline tools.

Every method takes an explicit *bucket*, so one store can serve several
buckets.  Keys and prefixes are bucket-relative and ``/``-delimited.  A
bundle's components (``data/``, ``changelog/``, ...) are the children of its
base prefix: list them with `BundleStore.list_children` and test one with
`BundleStore.prefix_exists` on ``<base_prefix>/<component>/``.
"""

from __future__ import annotations

import datetime
import threading
from collections.abc import Callable, Iterator
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

import boto3

from overture_core.cloud.aws import object as s3_object

_FOLDER_PLACEHOLDER = "$folder$"


@runtime_checkable
class BundleStore(Protocol):
    """Object storage operations the bundle model needs.

    Implementations must be safe to call from several threads at once:
    the batch lifecycle helpers share a store across a thread pool.
    """

    def exists(self, bucket: str, key: str) -> bool:
        """Return whether an object exists at exactly ``bucket/key``."""
        ...

    def last_modified(self, bucket: str, key: str) -> datetime.datetime | None:
        """Return when ``bucket/key`` was last written, or ``None`` if missing."""
        ...

    def prefix_exists(self, bucket: str, prefix: str) -> bool:
        """Return whether at least one object key starts with *prefix*.

        *prefix* is matched as-is; end it with ``/`` to test a directory.
        """
        ...

    def list_children(self, bucket: str, prefix: str) -> list[str]:
        """Return the immediate "subdirectory" names one level under *prefix*.

        Names exclude *prefix* and the trailing ``/`` and are sorted; empty
        segments and the ``$folder$`` placeholder are skipped.  *prefix* is
        matched as-is, so end it with ``/``.
        """
        ...

    def iter_keys(self, bucket: str, prefix: str) -> Iterator[str]:
        """Yield every object key that starts with *prefix*, recursively.

        *prefix* is matched as-is; end it with ``/`` to avoid matching
        sibling prefixes (``root/v=1`` also matches ``root/v=10``).
        """
        ...

    def read(self, bucket: str, key: str) -> bytes | None:
        """Return the body of ``bucket/key``, or ``None`` if it doesn't exist."""
        ...

    def write(self, bucket: str, key: str, body: bytes = b"") -> None:
        """Write *body* to ``bucket/key``, replacing any existing object."""
        ...

    def delete(self, bucket: str, key: str) -> None:
        """Delete ``bucket/key``; a missing key is not an error."""
        ...

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Delete every object under the directory *prefix* and return the count.

        *prefix* is treated as a directory: ``a/b`` deletes ``a/b/...`` but
        not ``a/bc``.

        Raises:
            ValueError: If *prefix* is empty, which would delete the bucket.
        """
        ...

    def presigned_url(self, bucket: str, key: str, *, expires_in: int = 3600) -> str:
        """Return a URL granting read access to ``bucket/key`` for *expires_in* seconds."""
        ...


class S3BundleStore:
    """`BundleStore` backed by S3 through `overture_core.cloud.aws.object`.

    Uses *client* if given, else creates one on first use and keeps it for
    the life of the store.  Each client has its own connection pool, so share
    one store across a unit of work (a walk, a cleanup, a batch check).
    """

    def __init__(self, *, client: Any = None) -> None:
        self._client = client
        self._lock = threading.Lock()

    @property
    def client(self) -> Any:
        """The boto3 S3 client, created on first access."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = boto3.client("s3")
        return self._client

    def exists(self, bucket: str, key: str) -> bool:
        return s3_object.object_exists(bucket, key, client=self.client)

    def last_modified(self, bucket: str, key: str) -> datetime.datetime | None:
        return s3_object.object_last_modified(bucket, key, client=self.client)

    def prefix_exists(self, bucket: str, prefix: str) -> bool:
        return s3_object.prefix_exists(bucket, prefix, client=self.client)

    def list_children(self, bucket: str, prefix: str) -> list[str]:
        return s3_object.list_common_prefixes(bucket, prefix, client=self.client)

    def iter_keys(self, bucket: str, prefix: str) -> Iterator[str]:
        return s3_object.iter_keys(bucket, prefix, client=self.client)

    def read(self, bucket: str, key: str) -> bytes | None:
        return s3_object.get_object_bytes(bucket, key, client=self.client)

    def write(self, bucket: str, key: str, body: bytes = b"") -> None:
        s3_object.put_object(bucket, key, body, client=self.client)

    def delete(self, bucket: str, key: str) -> None:
        s3_object.delete_object(bucket, key, client=self.client)

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        return s3_object.delete_prefix(bucket, prefix, client=self.client)

    def presigned_url(self, bucket: str, key: str, *, expires_in: int = 3600) -> str:
        return s3_object.presigned_url(
            bucket, key, expires_in=expires_in, client=self.client
        )


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class InMemoryBundleStore:
    """Dict-backed `BundleStore` for tests and offline tools.

    *clock* stamps each write (default: now, in UTC); `write` also takes
    ``last_modified=`` to seed one.  Listings are sorted, like S3's.
    `presigned_url` returns an opaque ``memory://`` URL.
    """

    def __init__(self, *, clock: Callable[[], datetime.datetime] | None = None) -> None:
        self._objects: dict[tuple[str, str], tuple[bytes, datetime.datetime]] = {}
        self._clock = clock or _utcnow
        self._lock = threading.Lock()

    def _keys(self, bucket: str, prefix: str) -> list[str]:
        with self._lock:
            return sorted(
                k for b, k in self._objects if b == bucket and k.startswith(prefix)
            )

    def exists(self, bucket: str, key: str) -> bool:
        with self._lock:
            return (bucket, key) in self._objects

    def last_modified(self, bucket: str, key: str) -> datetime.datetime | None:
        with self._lock:
            entry = self._objects.get((bucket, key))
        return entry[1] if entry else None

    def prefix_exists(self, bucket: str, prefix: str) -> bool:
        return bool(self._keys(bucket, prefix))

    def list_children(self, bucket: str, prefix: str) -> list[str]:
        names: set[str] = set()
        for key in self._keys(bucket, prefix):
            name, sep, _ = key[len(prefix) :].partition("/")
            if sep and name and name != _FOLDER_PLACEHOLDER:
                names.add(name)
        return sorted(names)

    def iter_keys(self, bucket: str, prefix: str) -> Iterator[str]:
        return iter(self._keys(bucket, prefix))

    def read(self, bucket: str, key: str) -> bytes | None:
        with self._lock:
            entry = self._objects.get((bucket, key))
        return entry[0] if entry else None

    def write(
        self,
        bucket: str,
        key: str,
        body: bytes = b"",
        *,
        last_modified: datetime.datetime | None = None,
    ) -> None:
        """Write *body* to ``bucket/key``, stamped *last_modified* or the clock."""
        stamp = last_modified if last_modified is not None else self._clock()
        with self._lock:
            self._objects[(bucket, key)] = (bytes(body), stamp)

    def delete(self, bucket: str, key: str) -> None:
        with self._lock:
            self._objects.pop((bucket, key), None)

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        prefix = prefix.strip("/")
        if not prefix:
            raise ValueError(
                f"Refusing to delete an empty prefix in bucket {bucket!r}: "
                "it would delete the whole bucket"
            )
        keys = self._keys(bucket, f"{prefix}/")
        with self._lock:
            for key in keys:
                self._objects.pop((bucket, key), None)
        return len(keys)

    def presigned_url(self, bucket: str, key: str, *, expires_in: int = 3600) -> str:
        return f"memory://{bucket}/{quote(key)}?expires_in={expires_in}"
