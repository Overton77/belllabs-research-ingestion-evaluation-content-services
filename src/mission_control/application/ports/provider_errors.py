"""Provider-neutral failure categories exposed by execution adapters."""


class ProviderTransportError(OSError):
    """A provider transport failed without proving whether the remote effect completed."""
