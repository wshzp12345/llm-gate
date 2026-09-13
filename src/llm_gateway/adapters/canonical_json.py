"""Canonicalization of validated semantic commands and resolved Snapshots.

Callers must first exclude headers/context and materialize domain defaults.
This codec is not a configuration validator or an ingress parser.
"""

import hashlib

import rfc8785


def canonical_bytes(value: object) -> bytes:
    return rfc8785.dumps(value)


def canonical_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()
