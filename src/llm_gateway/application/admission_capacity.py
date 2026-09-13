"""Process-owned instance/tenant permits for the fixed first-phase profile.

Only trusted Authorization Context supplies tenant identity. No Provider permit,
QPS budget, queue or durable Invocation identity is owned by this component.
"""

from threading import Lock

from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.domain.invocation import AuthorizationContext


class AdmissionLease:
    def __init__(self, owner, tenant):
        self._owner, self._tenant = owner, tenant
        self._entered = False

    def release(self):
        with self._owner._lock:
            if self not in self._owner._leases:
                return
            self._owner._leases.remove(self)
            self._owner._tenants[self._tenant] -= 1
            if self._owner._tenants[self._tenant] == 0:
                del self._owner._tenants[self._tenant]

    def __enter__(self):
        with self._owner._lock:
            if self not in self._owner._leases or self._entered:
                raise RuntimeError("Admission lease cannot be reused")
            self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False


class AdmissionCapacity:
    def __init__(self):
        self._lock = Lock()
        self._leases = set()
        self._tenants = {}
        self._accepting = True
        self._peak = self._instance_rejected = self._tenant_rejected = 0

    def metrics(self):
        with self._lock:
            return {"admission.in_use": len(self._leases), "admission.limit": 200,
                "admission.peak": self._peak, "admission.instance_rejected": self._instance_rejected,
                "admission.tenant_rejected": self._tenant_rejected}

    @property
    def in_use(self):
        with self._lock:
            return len(self._leases)

    def stop_admission(self):
        """Begin drain without revoking in-flight permits or waiting here."""
        with self._lock:
            self._accepting = False

    def acquire(self, authorization: AuthorizationContext) -> AdmissionLease:
        if not isinstance(authorization, AuthorizationContext):
            raise ValueError("Trusted Authorization Context required")
        tenant = authorization.tenant_id
        with self._lock:
            if not self._accepting:
                raise ModelInvocationRejected("gateway_not_ready")
            tenant_used = self._tenants.get(tenant, 0)
            if len(self._leases) >= 200 or tenant_used >= 50:
                if len(self._leases) >= 200:
                    self._instance_rejected += 1
                else:
                    self._tenant_rejected += 1
                raise ModelInvocationRejected("rate_limited")
            lease = AdmissionLease(self, tenant)
            self._leases.add(lease)
            self._peak = max(self._peak, len(self._leases))
            self._tenants[tenant] = tenant_used + 1
            return lease
