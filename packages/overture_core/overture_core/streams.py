"""Stream helpers for single-pass reads, e.g. ZIP entry to S3 uploads."""

from typing import IO


class NonSeekableReader:
    """Read-only wrapper that hides ``seek``/``tell`` from the wrapped file object.

    A ``zipfile.ZipFile.open`` entry reports ``seekable()`` as true, so boto3
    ``upload_fileobj`` seeks to the end to learn the size, decompressing the
    whole entry, then rewinds and decompresses it again. This wrapper reports
    ``seekable()`` as false, so s3transfer takes its streaming multipart path
    and reads the entry once.

    Args:
        fileobj: Binary file object to read from. Closed by the caller, not here.
    """

    def __init__(self, fileobj: IO[bytes]) -> None:
        self._fileobj = fileobj

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes (all remaining bytes if negative)."""
        return self._fileobj.read(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """Read bytes into ``buffer`` and return the number read."""
        return self._fileobj.readinto(buffer)

    def readable(self) -> bool:
        """Return True: the wrapper is read-only."""
        return True

    def seekable(self) -> bool:
        """Return False so consumers never seek or rewind."""
        return False

    def writable(self) -> bool:
        """Return False: the wrapper is read-only."""
        return False
