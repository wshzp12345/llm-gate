"""Immutable deny-by-default destination policy; no resolver or socket I/O."""

import ipaddress
import re
from dataclasses import dataclass, field


class EgressPolicyViolation(PermissionError):
    def __init__(self):
        super().__init__("Provider egress policy rejected connection")


def _address(value):
    if not isinstance(value, str) or "%" in value:
        raise EgressPolicyViolation()
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        raise EgressPolicyViolation() from None


def _public(address):
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return _public(address.ipv4_mapped)
    # Global multicast and reserved destinations are not public unicast.
    if not address.is_global or address.is_multicast or address.is_reserved:
        return False
    if isinstance(address, ipaddress.IPv6Address):
        if address.sixtofour is not None:
            return _public(address.sixtofour)
        if address.teredo is not None:
            return all(_public(part) for part in address.teredo)
    return True


@dataclass(frozen=True)
class ProviderEgressPolicy:
    allowed_hosts: tuple[str, ...] = field(repr=False)
    allowed_networks: tuple[str, ...] = field(repr=False)

    def __post_init__(self):
        for entries in (self.allowed_hosts, self.allowed_networks):
            if not isinstance(entries, tuple) or not entries or len(set(entries)) != len(entries):
                raise ValueError("Immutable nonempty unique egress allowlist required")
        for host in self.allowed_hosts:
            if (not isinstance(host, str) or len(host) > 253 or
                    not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in host.split("."))):
                raise ValueError("Canonical DNS hostname required")
            try:
                ipaddress.ip_address(host)
            except ValueError:
                pass
            else:
                raise ValueError("Hostname allowlist cannot contain IP literals")
        for network in self.allowed_networks:
            if network != "public":
                try:
                    if str(ipaddress.ip_network(network, strict=True)) != network:
                        raise ValueError()
                except (ValueError, TypeError):
                    raise ValueError("Canonical CIDR required") from None

    def validate_addresses(self, host: str, addresses: tuple[str, ...]) -> tuple[str, ...]:
        if host not in self.allowed_hosts or not isinstance(addresses, tuple) or not addresses:
            raise EgressPolicyViolation()
        networks = tuple(ipaddress.ip_network(value) for value in self.allowed_networks if value != "public")
        checked = []
        for value in addresses:
            address = _address(value)
            if not ("public" in self.allowed_networks and _public(address)
                    or any(address.version == network.version and address in network for network in networks)):
                raise EgressPolicyViolation()
            checked.append(str(address))
        # Selection is allowed only after validating the entire answer set.
        return tuple(dict.fromkeys(checked))
