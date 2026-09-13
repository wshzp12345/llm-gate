"""HTTP/1.1 connection age limits without cancelling active response streams."""

import time

import httpcore


class ProviderTransportRetired(PermissionError):
    def __init__(self):
        super().__init__("Provider transport no longer accepts requests")


class _LifetimeConnection(httpcore.AsyncConnectionInterface):
    def __init__(self, connection, lifetime, clock, accepting):
        self._connection, self._clock = connection, clock
        self._deadline = clock() + lifetime
        self._used = False
        self._accepting = accepting

    def _aged(self):
        return self._clock() >= self._deadline

    async def handle_async_request(self, request):
        # Assignment and execution can be separated by pool cleanup awaits.
        if not self._accepting():
            raise ProviderTransportRetired()
        if self._used and self._aged():
            raise httpcore.ConnectionNotAvailable()
        self._used = True
        return await self._connection.handle_async_request(request)

    def is_available(self):
        return self._accepting() and not self._aged() and self._connection.is_available()

    def has_expired(self):
        # httpcore immediately closes expired connections, including active ones.
        return self._connection.has_expired() or ((self._aged() or not self._accepting()) and self.is_idle())

    def can_handle_request(self, origin):
        return self._connection.can_handle_request(origin)

    def is_idle(self):
        return self._connection.is_idle()

    def is_closed(self):
        return (not self._used and not self._accepting()) or self._connection.is_closed()

    def info(self):
        return self._connection.info()

    async def aclose(self):
        await self._connection.aclose()


class EgressConnectionPool(httpcore.AsyncConnectionPool):
    def __init__(self, *, max_connection_lifetime_seconds=300, clock=time.monotonic, **kwargs):
        if (type(max_connection_lifetime_seconds) is not int
                or not 30 <= max_connection_lifetime_seconds <= 3600):
            raise ValueError("Published connection lifetime must be between 30 and 3600 seconds")
        if kwargs.get("http2", False) or kwargs.get("http1", True) is not True:
            raise ValueError("Provider connection lifetime requires HTTP/1.1")
        super().__init__(**kwargs)
        self._lifetime, self._clock = max_connection_lifetime_seconds, clock
        self._accepting_requests = True

    def create_connection(self, origin):
        # Already queued requests may be assigned during httpcore cleanup.
        # A retired wrapper rejects them before DNS without breaking that cleanup.
        return _LifetimeConnection(super().create_connection(origin), self._lifetime, self._clock,
                                   lambda: self._accepting_requests)

    async def handle_async_request(self, request):
        if not self._accepting_requests:
            raise ProviderTransportRetired()
        return await super().handle_async_request(request)

    def stop_acquiring(self):
        """Synchronous publication fence, before any asynchronous idle cleanup."""
        self._accepting_requests = False

    async def close_idle(self):
        if self._accepting_requests:
            raise RuntimeError("Retire the Provider pool before closing idle connections")
        # Public snapshot only; httpcore retains ownership of its connection list.
        for connection in self.connections:
            if connection.is_idle():
                await connection.aclose()

    @property
    def drained(self):
        return not self._accepting_requests and all(connection.is_closed() for connection in self.connections)

    async def aclose(self):
        self.stop_acquiring()
        await super().aclose()
