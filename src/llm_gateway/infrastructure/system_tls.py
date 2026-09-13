"""System trust without SSL_CERT_FILE/SSL_CERT_DIR environment overrides."""

from pathlib import Path
import ssl


def system_tls_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    # Windows roots are exposed by Python; Unix roots use OpenSSL's compiled
    # paths, not the environment-resolved cafile/capath fields.
    if hasattr(ssl, "enum_certificates"):
        for store in ("CA", "ROOT"):
            for certificate, encoding, trust in ssl.enum_certificates(store):
                if encoding == "x509_asn" and (trust is True or ssl.Purpose.SERVER_AUTH.oid in trust):
                    context.load_verify_locations(cadata=certificate)
    paths = ssl.get_default_verify_paths()
    cafile = paths.openssl_cafile if paths.openssl_cafile and Path(paths.openssl_cafile).is_file() else None
    capath = paths.openssl_capath if paths.openssl_capath and Path(paths.openssl_capath).is_dir() else None
    if cafile is not None or capath is not None:
        context.load_verify_locations(cafile=cafile, capath=capath)
    if not context.get_ca_certs() and capath is None:
        raise RuntimeError("No system Provider trust roots are available")
    return context
