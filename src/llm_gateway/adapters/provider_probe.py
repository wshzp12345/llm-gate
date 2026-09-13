"""Safe-path GET probe on an operational, TLS/Egress-controlled HTTP client.

The composition owns a separate credential resolver and HTTP connection pool.
No live Invocation, user headers, response body, Usage or cost is used here.
"""

import json
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from llm_gateway.adapters.openai_compatible import _network_retryable
from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.application.provider_probe import ProbeOutcome, ProbePolicy


def project_probe_policy(snapshot, provider_id):
    content = json.loads(snapshot.snapshot_json)
    probe = content["providers"][provider_id]["health"]["active_probe"]
    if probe is None:
        return None
    return ProbePolicy(provider_id, snapshot.revision, probe["interval_seconds"], probe["timeout_seconds"])


class HttpProviderProbe:
    def __init__(self, *, client, source, base_url: str, path: str, secret_ref: str,
                 utcnow=lambda: datetime.now(timezone.utc)):
        root = httpx.URL(base_url)
        if (root.scheme not in {"http", "https"} or not root.host or root.userinfo
                or root.query or root.fragment or base_url.endswith("/")):
            raise ValueError("Configured Provider API root required")
        if (not isinstance(path, str) or not path.startswith("/") or path.startswith("//")
                or "\\" in path or "#" in path or any(ord(char) <= 32 for char in path)):
            raise ValueError("Configured same-origin probe path required")
        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            raise ValueError("Configured same-origin probe path required")
        origin = root.copy_with(path="/", query=None, fragment=None)
        target = origin.join(path)
        if (target.scheme, target.host, target.port) != (root.scheme, root.host, root.port):
            raise ValueError("Probe origin mismatch")
        if not isinstance(secret_ref, str) or not secret_ref:
            raise ValueError("Explicit probe Secret Reference required")
        self._client, self._source, self._url = client, source, target
        self._secret_ref, self._utcnow = secret_ref, utcnow

    async def probe(self, context) -> ProbeOutcome:
        try:
            lease = await self._source.resolve(self._secret_ref)
        except CredentialUnavailable:
            return ProbeOutcome("provider_credentials_unavailable")
        except Exception:
            return ProbeOutcome("internal")
        try:
            with lease:
                metadata = lease.metadata
                if metadata.secret_ref != self._secret_ref or metadata.revoked or metadata.valid_until <= self._utcnow():
                    return ProbeOutcome("provider_credentials_unavailable")
                # The runner owns the single timeout across resolution and I/O.
                # Never follow redirects or consume a potentially unbounded body.
                request = httpx.Request("GET", self._url,
                    headers={"Authorization": "Bearer " + lease.bearer_value()},
                    extensions={"timeout": httpx.Timeout(None).as_dict()})
                context.begin_transport()
                response = await self._client.send(request, stream=True, follow_redirects=False, auth=None)
                context.finish_transport()
                try:
                    status = response.status_code
                    if 200 <= status < 300:
                        return ProbeOutcome("available")
                    if status in {401, 403}:
                        return ProbeOutcome("provider_credentials_unavailable")
                    if status == 429:
                        return ProbeOutcome("rate_limited")
                    if status in {500, 502, 503, 504}:
                        return ProbeOutcome("provider_unavailable")
                    return ProbeOutcome("provider_protocol_error")
                finally:
                    await response.aclose()
        except CredentialUnavailable:
            return ProbeOutcome("provider_credentials_unavailable")
        except httpx.TimeoutException:
            return ProbeOutcome(context.timeout_code)
        except httpx.NetworkError as error:
            return ProbeOutcome("internal" if context.timeout_code != "upstream_timeout" else
                "provider_unavailable" if _network_retryable(error) else "transport_nonretryable")
        except httpx.HTTPError:
            return ProbeOutcome("provider_protocol_error")
        except Exception:
            return ProbeOutcome("internal")
