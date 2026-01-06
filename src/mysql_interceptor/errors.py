from __future__ import annotations


class InterceptorError(Exception):
    """Base error."""


class DriverAdapterError(InterceptorError):
    """Raised when a DB driver adapter cannot be used."""


class PublisherError(InterceptorError):
    """Raised when a publisher cannot be initialized."""
