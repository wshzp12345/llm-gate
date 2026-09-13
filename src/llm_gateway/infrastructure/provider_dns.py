"""Bounded off-loop DNS without a cache or cancellation-created spare capacity."""

import asyncio
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from llm_gateway.adapters.provider_dns import DnsResolutionFailure, DnsResolverInternalError
from llm_gateway.application.concurrency import ConcurrencyPool


class BoundedDnsResolver:
    def __init__(self, *, max_concurrent_lookups: int, max_addresses: int = 128, lookup=socket.getaddrinfo):
        if type(max_addresses) is not int or max_addresses < 1:
            raise ValueError("Positive DNS answer ceiling required")
        self._pool = ConcurrencyPool(max_concurrent_lookups)
        self._workers = ThreadPoolExecutor(max_workers=max_concurrent_lookups, thread_name_prefix="gateway-dns")
        self._lookup, self._max_addresses = lookup, max_addresses
        self._lock, self._pending, self._closed = Lock(), set(), False

    def _resolve(self, host, port):
        answers = self._lookup(host, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
        if not answers or len(answers) > self._max_addresses:
            raise DnsResolutionFailure(False)
        addresses = []
        for family, kind, protocol, _, address in answers:
            if (family not in {socket.AF_INET, socket.AF_INET6} or kind != socket.SOCK_STREAM
                    or protocol not in {0, socket.IPPROTO_TCP} or address[1] != port
                    or family == socket.AF_INET6 and address[3] != 0):
                raise DnsResolutionFailure(False)
            addresses.append(address[0])
        return tuple(addresses)

    async def resolve(self, host, port):
        with self._lock:
            permit = None if self._closed else self._pool.try_acquire()
            if permit is None:
                raise DnsResolutionFailure(False)
            try:
                future = self._workers.submit(self._resolve, host, port)
            except RuntimeError:
                permit.release()
                raise DnsResolutionFailure(False) from None
            self._pending.add(future)
            future.add_done_callback(lambda _: permit.release())
        try:
            try:
                return await asyncio.wrap_future(future)
            except DnsResolutionFailure:
                raise
            except socket.gaierror as error:
                raise DnsResolutionFailure(error.errno == socket.EAI_AGAIN) from None
            except TimeoutError:
                raise DnsResolutionFailure(True) from None
            except Exception:
                raise DnsResolverInternalError() from None
        finally:
            with self._lock:
                self._pending.discard(future)

    def close(self):
        with self._lock:
            self._closed = True
            for future in self._pending:
                future.cancel()
        self._workers.shutdown(wait=False, cancel_futures=True)
