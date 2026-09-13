import socket
import ssl

import httpx
import pytest

from llm_gateway.domain.model import FailureCode, ProviderFailure
from tests.test_completion import invoke


@pytest.mark.parametrize("cause,retryable", [
    (ssl.SSLCertVerificationError(1, "sensitive certificate"), False),
    (ssl.SSLError(1, "sensitive TLS configuration"), False),
    (PermissionError("sensitive local path"), False),
    (socket.gaierror(socket.EAI_NONAME, "sensitive hostname"), False),
    (socket.gaierror(socket.EAI_AGAIN, "sensitive hostname"), True),
    (ssl.SSLEOFError(1, "sensitive endpoint"), True),
    (ConnectionResetError("sensitive endpoint"), True),
])
@pytest.mark.parametrize("implicit", [False, True, "suppressed"])
def test_typed_transport_causes_control_retry_without_disclosure(cause, retryable, implicit):
    calls = []

    def handler(request):
        calls.append(request)
        wrapped = RuntimeError("sensitive library wrapper")
        wrapped.__cause__ = cause
        try:
            raise wrapped
        except RuntimeError:
            if implicit == "suppressed":
                raise httpx.ConnectError("sensitive endpoint", request=request) from None
            if implicit:
                raise httpx.ConnectError("sensitive endpoint", request=request)
            raise httpx.ConnectError("sensitive endpoint", request=request) from wrapped

    result = invoke(handler)
    assert result == ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, retryable)
    assert "sensitive" not in repr(result)
    assert len(calls) == 1


@pytest.mark.parametrize("cyclic", [False, True])
def test_unbounded_cause_chains_fail_closed(cyclic):
    root = RuntimeError("private")
    current = root
    for _ in range(20):
        current.__cause__ = RuntimeError("private")
        current = current.__cause__
    if cyclic:
        root.__cause__ = root

    def handler(request):
        raise httpx.ConnectError("private", request=request) from root

    assert invoke(handler) == ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)


@pytest.mark.parametrize("wrapped", [ssl.SSLCertVerificationError("private"), PermissionError("private"),
    (ConnectionResetError("private"), ConnectionResetError("private"))])
def test_typed_exception_arguments_do_not_grant_unsafe_or_ambiguous_retry(wrapped):
    def handler(request):
        error = httpx.ConnectError("private", request=request)
        error.args = wrapped if isinstance(wrapped, tuple) else (wrapped,)
        raise error from None
    assert invoke(handler) == ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)
