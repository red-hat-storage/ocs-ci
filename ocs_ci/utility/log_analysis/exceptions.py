"""
Exceptions for the log analysis module.
"""


class LogAnalysisError(Exception):
    """Base exception for log analysis errors."""

    pass


class ArtifactFetchError(LogAnalysisError):
    """Failed to fetch or parse log artifacts."""

    pass


class JUnitParseError(LogAnalysisError):
    """Failed to parse JUnit XML."""

    pass


class AIBackendError(LogAnalysisError):
    """AI backend call failed."""

    pass


class CacheError(LogAnalysisError):
    """Cache read/write failed."""

    pass
