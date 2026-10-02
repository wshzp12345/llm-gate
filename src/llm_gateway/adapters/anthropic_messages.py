"""Anthropic Messages to the Gateway-owned synchronous text Provider Port."""

import json
from collections.abc import Mapping
from dataclasses import replace

import httpx

from llm_gateway.adapters.openai_compatible import (
    OpenAICompatibleCompletion, _count, _network_retryable, _reject_constant, _unique_object,
)
from llm_gateway.adapters.retry_after import provider_retry_after_ms
from llm_gateway.domain.model import CompletionRequest, FailureCode, ProviderFailure, ProviderResult, RefusalOutput, TextOutput, Usage


class AnthropicMessagesCompletion(OpenAICompatibleCompletion):
    """Keep Messages wire details outside the application and domain layers."""

    def __init__(self, client: httpx.AsyncClient, *, base_url: str, credential: str,
                 max_response_bytes: int = 4 * 1024 * 1024, clock=None):
        kwargs = {"clock": clock} if clock is not None else {}
        super().__init__(client, base_url=base_url, credential=credential,
                         max_response_bytes=max_response_bytes, **kwargs)
        self._url = base_url + "/messages"

    async def stream(self, request: CompletionRequest, *, context=None):
        raise ValueError("Anthropic Messages streaming is not enabled by this Adapter contract")
        yield  # This is intentionally an async generator like the Provider Port.

    @staticmethod
    def _http_failure(status: int) -> ProviderFailure | None:
        if status == 529:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, True)
        return OpenAICompatibleCompletion._http_failure(status)

    async def _complete_request(self, request: CompletionRequest) -> ProviderResult | ProviderFailure:
        system = []
        messages = []
        for message in request.messages:
            if message.role in {"system", "developer"}:
                if messages:
                    return ProviderFailure(FailureCode.INVALID_REQUEST, False)
                system.append(message.text)
            else:
                messages.append({"role": message.role, "content": message.text})
        if not messages:
            return ProviderFailure(FailureCode.INVALID_REQUEST, False)
        payload = {"model": request.resolved_model, "max_tokens": request.max_output_tokens,
                   "messages": messages, "stream": False}
        if system:
            payload["system"] = "\n\n".join(system)
        if request.temperature is not None:
            payload["temperature"] = request.temperature
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        if request.output_format is not None:
            schema = ({"type": "object"} if request.output_format.type == "json_object"
                      else json.loads(request.output_format.schema_json))
            payload["output_config"] = {"format": {"type": "json_schema", "schema": schema}}
        try:
            async with self._client.stream("POST", self._url, json=payload,
                headers={"x-api-key": self._credential, "anthropic-version": "2023-06-01"},
                follow_redirects=False) as response:
                failure = self._http_failure(response.status_code)
                if failure is not None:
                    if failure.retryable:
                        failure = replace(failure, retry_after_ms=provider_retry_after_ms(response.headers, now=self._clock))
                    return failure
                if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
                    return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > self._max_response_bytes:
                        return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
                    body.extend(chunk)
                data = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object,
                                  parse_constant=_reject_constant)
                return self._translate(data, request)
        except httpx.PoolTimeout:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)
        except httpx.ConnectTimeout:
            return ProviderFailure(FailureCode.UPSTREAM_TIMEOUT, True)
        except httpx.TimeoutException:
            return ProviderFailure(FailureCode.UNCERTAIN, False)
        except (httpx.LocalProtocolError, httpx.UnsupportedProtocol, httpx.DecodingError):
            return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
        except httpx.ConnectError as error:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, _network_retryable(error))
        except httpx.TransportError:
            return ProviderFailure(FailureCode.UNCERTAIN, False)
        except (ValueError, KeyError, TypeError, IndexError, RecursionError):
            return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)

    @staticmethod
    def _translate(data: object, request: CompletionRequest) -> ProviderResult:
        if not isinstance(data, Mapping) or data.get("type") != "message" or data.get("role") != "assistant":
            raise ValueError("Invalid Messages envelope")
        model, blocks, stop = data["model"], data["content"], data["stop_reason"]
        if not isinstance(model, str) or not model or not isinstance(blocks, list):
            raise ValueError("Invalid Messages fields")
        if any(not isinstance(block, dict) or block.get("type") != "text" or not isinstance(block.get("text"), str)
               for block in blocks):
            raise ValueError("Unsupported Messages content")
        content = "".join(block["text"] for block in blocks)
        content.encode("utf-8")
        raw_usage = data.get("usage")
        if not isinstance(raw_usage, dict):
            usage = Usage()
        else:
            uncached = _count(raw_usage.get("input_tokens"))
            created = _count(raw_usage.get("cache_creation_input_tokens", 0))
            cached = _count(raw_usage.get("cache_read_input_tokens", 0))
            total_input = sum((uncached, created, cached)) if None not in (uncached, created, cached) else None
            usage = Usage(total_input, _count(raw_usage.get("output_tokens")), cached if "cache_read_input_tokens" in raw_usage else None,
                          _count(raw_usage.get("output_tokens_details", {}).get("thinking_tokens"))
                          if isinstance(raw_usage.get("output_tokens_details"), dict) else None)
        details = data.get("stop_details")
        if stop == "refusal" or isinstance(details, dict) and details.get("type") == "refusal":
            return ProviderResult(request.requested_model, model, RefusalOutput(content, None), usage, "content_filter")
        if stop not in {"end_turn", "max_tokens", "stop_sequence"}:
            raise ValueError("Unsupported Messages stop reason")
        return ProviderResult(request.requested_model, model, TextOutput(content), usage,
                              "length" if stop == "max_tokens" else "stop")
