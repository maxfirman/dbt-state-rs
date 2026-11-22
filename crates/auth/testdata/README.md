# Test-only signing keys

These RSA keys exist only so tests can sign and verify tokens. They protect
nothing and must never be used by a deployment.

| File | Use |
|---|---|
| `signing-key-1.pem` | PKCS#8, the test config's active key (`test-key-1`) |
| `signing-key-2.pem` | PKCS#8, a second loaded key (`test-key-2`) |
| `signing-key-pkcs1.pem` | PKCS#1 (`BEGIN RSA PRIVATE KEY`), loads like PKCS#8 |
| `untrusted-key.pem` | never in a server config; signs "bad signature" tokens |
| `upstream-idp-key.pem` | the harness's fake Upstream IdP signs its access tokens with it |

Regenerate with `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048`
(`openssl genrsa -traditional 2048` for the PKCS#1 key).
