from dataclasses import dataclass
from enum import Enum


class CloudProvider(Enum):
    """Supported cloud providers for data locations."""

    AWS = "aws"
    AZURE = "azure"


@dataclass
class Partition:
    """Represents a partition in a data location path.

    Can be Hive-style (key=value) or simple (value only).

    Attributes:
        key: Partition key for Hive-style paths (e.g., 'ds', 'date')
        delimiter: Separator between key and value (typically '=')
        value: Partition value (can be Jinja2 template, e.g., "{{ params.ds }}")

    Examples:
        # Hive-style partition
        Partition(key="ds", delimiter="=", value="2024-08-01")
        # Result in path: "ds=2024-08-01"

        # Simple partition (no key/delimiter)
        Partition(value="2024-08-01")
        # Result in path: "2024-08-01"

        # No partition
        Partition()
        # Result in path: nothing
    """

    key: str = ""
    delimiter: str = ""
    value: str = ""

    def is_empty(self) -> bool:
        """Check if partition has no value."""
        return not self.value

    def is_hive_style(self) -> bool:
        """Check if this is a Hive-style partition."""
        return bool(self.key and self.delimiter and self.value)

    def build(self) -> str:
        """Build the partition path component.

        Returns:
            Partition path string, or empty string if no value
        """
        if not self.value:
            return ""
        if self.key and self.delimiter:
            return f"{self.key}{self.delimiter}{self.value}"
        return self.value
