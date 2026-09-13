"""Content-free async DNS boundary for direct Provider connections."""

from typing import Protocol


class DnsResolutionFailure(Exception):
    def __init__(self, retryable: bool):
        self.retryable = retryable
        super().__init__("Provider DNS resolution unavailable")


class DnsResolverInternalError(RuntimeError):
    def __init__(self):
        super().__init__("Provider DNS resolver failed")


class ProviderDnsResolver(Protocol):
    async def resolve(self, host: str, port: int) -> tuple[str, ...]: ...
