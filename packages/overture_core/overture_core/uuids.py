"""Pure Python UUID generation helpers.

No Spark dependency by design: callers on the Spark side wrap these in an
`@F.udf`/`@udf` decorator themselves. Centralizes UUID generation that was
previously hand-rolled ad hoc across several tf-data-platform call sites
(some via Spark SQL's `uuid()`, some via Python's `uuid` module directly).

`generate_uuid6`/`generate_uuid7`/`generate_uuid8` wrap `uuid.uuid6`/`uuid7`/
`uuid8` (RFC 9562), which the standard library only ships starting with
Python 3.14. This package supports Python 3.11+, so those wrappers resolve
the stdlib function lazily at call time via getattr and raise
NotImplementedError on older interpreters, instead of failing at import time
for everyone else.
"""

import sys
import uuid


def generate_uuid3(namespace: uuid.UUID, name: str) -> str:
    """Generate a name-based UUID (MD5) as a string.

    >>> generate_uuid3(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "example.com")
    '9073926b-929f-31c2-abc9-fad77ae3e8eb'
    """
    return str(uuid.uuid3(namespace, name))


def generate_uuid4() -> str:
    """Generate a random UUID as a string."""
    return str(uuid.uuid4())


def generate_uuid5(namespace: uuid.UUID, name: str) -> str:
    """Generate a name-based UUID (SHA-1) as a string.

    >>> generate_uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "example.com")
    'cfbff0d1-9375-5685-968c-48ce8b15ae17'
    """
    return str(uuid.uuid5(namespace, name))


def _stdlib_uuid_generator(name: str):
    """Look up uuid.<name> on the running interpreter, if it exists.

    Backs the uuid6/7/8 wrappers below: those RFC 9562 generators only exist
    in the stdlib `uuid` module on Python 3.14+, so the lookup happens here,
    at call time, rather than as a module-level import that would break
    import of this whole module on 3.11-3.13.
    """
    generator = getattr(uuid, name, None)
    if generator is None:
        py_version = ".".join(map(str, sys.version_info[:3]))
        raise NotImplementedError(
            f"generate_{name} requires Python 3.14+ (uuid.{name} was added by "
            f"RFC 9562 support); the running interpreter is {py_version}"
        )
    return generator


def generate_uuid6(node: int | None = None, clock_seq: int | None = None) -> str:
    """Generate a time-ordered (Gregorian, reordered-fields) UUID as a string.

    Requires Python 3.14+; raises NotImplementedError on older interpreters.
    """
    return str(_stdlib_uuid_generator("uuid6")(node=node, clock_seq=clock_seq))


def generate_uuid7() -> str:
    """Generate a Unix-timestamp-ordered UUID as a string.

    Requires Python 3.14+; raises NotImplementedError on older interpreters.
    """
    return str(_stdlib_uuid_generator("uuid7")())


def generate_uuid8(
    a: int | None = None, b: int | None = None, c: int | None = None
) -> str:
    """Generate an application-defined (custom-bits) UUID as a string.

    `a`, `b`, `c` are the caller-supplied custom bit fields defined by RFC
    9562 (48/12/62 bits respectively); any left as None are filled with
    random bits. Requires Python 3.14+; raises NotImplementedError on older
    interpreters.
    """
    return str(_stdlib_uuid_generator("uuid8")(a, b, c))
