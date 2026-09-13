"""Synthetic configuration data, with no real endpoint or credential."""


def bundle_submission():
    return {"kind": "bundle", "bundle": {
        "schema_version": "gateway.config/v1", "base_revision": "0",
        "providers": {"provider-a": {
            "status": "enabled", "adapter": {"type": "compatible", "version": "v1"},
            "endpoint": {"base_url": "https://provider.invalid/v1"},
            "credential": {"secret_ref": "test-provider-reference"},
            "egress": {"allowed_hosts": ["provider.invalid"], "allowed_networks": ["public"], "proxy": "disabled"},
            "transport": {"http_version": "1.1", "tls": {"trust": "system", "trust_bundle": None}},
            "health": {"active_probe": None}, "rate_limit": {"max_concurrency": 2, "qps": 2, "burst": 2},
        }},
        "provider_model_bindings": {"binding-a": {
            "status": "enabled", "provider": "provider-a", "upstream_model": "Upstream-Model",
            "capabilities": {"streaming": False, "tool_calling": False, "structured_output": "none"},
            "limits": {"context_tokens": 8192, "max_output_tokens": 4096}, "pricing_table": "price-a",
        }},
        "model_aliases": {"general": {
            "candidates": [{"binding": "binding-a", "service_level": "full", "priority": 0, "weight": 1}],
            "routing_policy": "route-a", "safety_policy": "safety-a", "generation_defaults": {}, "structured_output": {},
        }},
        "routing_policies": {"route-a": {
            "selection": {"strategy": "priority_weighted_without_replacement", "seed_profile": "gateway.routing-seed/v1"},
            "retry": {"max_attempts": 3, "max_attempts_per_candidate": 2, "base_delay_ms": 100, "multiplier": 2,
                      "max_delay_ms": 1000, "jitter": "full", "max_retry_after_seconds": 5},
            "fallback": {"mode": "pre_commit_only"},
            "degradation": {"reduced_service": {"enabled": False}, "cache": {"enabled": False},
                            "terminal": {"mode": "preserve_live_failure"}},
        }},
        "pricing_tables": {"price-a": {
            "currency": "USD", "unit": "per_million_tokens", "effective_from": "2026-09-07T00:00:00Z",
            "rates": {"input": "1.25", "output": "2", "cached_input": None, "reasoning_output": None},
            "rounding": "half_even_12dp",
        }},
        "safety_policies": {"safety-a": {"mode": "provider_refusal_terminal"}},
        "resource_policies": {"structured_output": {}, "idempotency": {}, "limits": {}},
    }}
