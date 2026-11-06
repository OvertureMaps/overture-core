"""Let the stdlib ``zipfile`` read Deflate64 (compression method 9) entries.

``zipfile`` raises ``NotImplementedError`` on Deflate64, which Windows-built
archives (e.g. the NAD r24 zip) use, and it has no plugin hook for extra
methods. :func:`enable_zipfile_deflate64` therefore wraps the private
``zipfile._get_decompressor`` for the whole process, so a Python release that
changes that function can break it. Only reading is patched: ``zipfile``
still can't write Deflate64.

``inflate64`` ships compiled wheels, so it's in the optional ``compression``
extra (``overture-core[compression]``), imported lazily: importing this module
needs only the standard library.
"""

import zipfile

ZIP_DEFLATE64 = 9


def enable_zipfile_deflate64() -> None:
    """Patch ``zipfile`` to read Deflate64 entries via ``inflate64``. Idempotent.

    Other compression methods still go to the original decompressor. The
    ``zipfile._overture_deflate64`` flag marks the patch as applied, so later
    calls, even from another copy of this patch that checks the same flag,
    are no-ops.

    Raises:
        ImportError: If the ``compression`` extra (``inflate64``) isn't installed.
    """
    if getattr(zipfile, "_overture_deflate64", False):
        return

    try:
        import inflate64
    except ModuleNotFoundError as exc:
        raise ImportError(
            "enable_zipfile_deflate64 requires the 'compression' extra: "
            "install with `pip install 'overture-core[compression]'`"
        ) from exc

    class _Deflate64Decompressor:
        def __init__(self) -> None:
            self._inflater = inflate64.Inflater()

        def decompress(self, data: bytes) -> bytes:
            try:
                return self._inflater.inflate(data)
            except ValueError as exc:
                raise zipfile.BadZipFile(f"Corrupt Deflate64 stream: {exc}") from exc

        @property
        def eof(self) -> bool:
            return self._inflater.eof

    orig_get = zipfile._get_decompressor

    def _get_decompressor(compress_type):
        if compress_type == ZIP_DEFLATE64:
            return _Deflate64Decompressor()
        return orig_get(compress_type)

    zipfile._get_decompressor = _get_decompressor
    zipfile._overture_deflate64 = True
