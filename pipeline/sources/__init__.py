"""Data source provider registry."""

from .base import DataSourceProvider, SourceOverrides, PollStrategy
from .factory import get_source_provider
from .galileosky import GalileoskySourceProvider
from .wialon_ips import WialonIPSSourceProvider

__all__ = [
    "DataSourceProvider",
    "SourceOverrides",
    "PollStrategy",
    "GalileoskySourceProvider",
    "WialonIPSSourceProvider",
    "get_source_provider",
]
