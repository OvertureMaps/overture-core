"""Azure Blob Storage helpers.

``azure-storage-blob`` is imported lazily inside each function so it stays an
optional dependency (``overture-core[azure]``) and never taxes import time
for consumers that don't touch Azure.
"""

import logging
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


def blob_account_url(storage_account: str) -> str:
    """Return the Blob service endpoint for *storage_account*."""
    return f"https://{storage_account}.blob.core.windows.net"


@dataclass(frozen=True)
class ContentMd5ClearResult:
    """Outcome of :func:`clear_content_md5`."""

    scanned: int
    had_md5: int
    cleared: int
    errors: list[str]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["errors"] = len(self.errors)
        return data


def clear_content_md5(
    storage_account: str,
    container: str,
    prefix: str,
    credential,
) -> ContentMd5ClearResult:
    """Clear the ``Content-MD5`` property on every blob under *prefix*.

    AWS DataSync stamps an incorrect ``Content-MD5`` on multi-block Azure blobs
    (correct for single-block, absent above ~1 GB), which makes ``azcopy`` and
    Storage Explorer downloads hard-fail on verification. The data itself is
    fine; only the property is wrong. Clearing it is safe: ``azcopy``'s default
    ``--check-md5 FailIfDifferent`` passes when the property is absent, and no
    bytes move. The ``az`` CLI can't do this (``--content-md5 ""`` is a silent
    no-op), so the SDK call is required.

    The blob's other content settings are preserved; a bare ``set_http_headers``
    would otherwise wipe them.

    Args:
        storage_account: Azure Storage account name.
        container: Blob container name.
        prefix: Blob-name prefix to scan; may be empty for the whole container.
        credential: Anything ``BlobServiceClient`` accepts, e.g. a SAS token string.

    Raises:
        RuntimeError: If any blob fails to update. The returned result is
            still available on the exception's ``result`` attribute.
    """
    from azure.storage.blob import BlobServiceClient, ContentSettings

    container_client = BlobServiceClient(
        account_url=blob_account_url(storage_account), credential=credential
    ).get_container_client(container)

    name_starts_with = prefix.strip("/")
    name_starts_with = f"{name_starts_with}/" if name_starts_with else ""

    scanned = had_md5 = cleared = 0
    errors: list[str] = []
    for blob in container_client.list_blobs(name_starts_with=name_starts_with):
        scanned += 1
        settings = blob.content_settings
        if not (settings and settings.content_md5):
            continue
        had_md5 += 1
        try:
            container_client.get_blob_client(blob.name).set_http_headers(
                content_settings=ContentSettings(
                    content_type=settings.content_type,
                    content_encoding=settings.content_encoding,
                    content_language=settings.content_language,
                    content_disposition=settings.content_disposition,
                    cache_control=settings.cache_control,
                    content_md5=None,
                )
            )
            cleared += 1
        except Exception as exc:
            errors.append(f"{blob.name}: {exc}")

    result = ContentMd5ClearResult(
        scanned=scanned, had_md5=had_md5, cleared=cleared, errors=errors
    )
    logger.info(
        "Content-MD5 clear for %s/%s: %d scanned, %d had md5, %d cleared, %d errors",
        container,
        name_starts_with,
        scanned,
        had_md5,
        cleared,
        len(errors),
    )
    if errors:
        exc = RuntimeError(
            f"Failed to clear Content-MD5 on {len(errors)} blob(s); "
            f"first failure: {errors[0]}"
        )
        exc.result = result
        raise exc
    return result
