# Test-only keys and certificates

These RSA keys exist only so tests can sign and verify tokens. They protect
nothing and must never be used by a deployment.

| File | Use |
|---|---|
| `signing-key-1.pem` | PKCS#8, the test config's active key (`test-key-1`) |
| `signing-key-2.pem` | PKCS#8, a second loaded key (`test-key-2`) |
| `signing-key-pkcs1.pem` | PKCS#1 (`BEGIN RSA PRIVATE KEY`), loads like PKCS#8 |
| `untrusted-key.pem` | never in a server config; signs "bad signature" tokens |
| `upstream-idp-key.pem` | the harness's fake Upstream IdP signs its access tokens with it |
| `upstream-rp-key.pem` | the Token broker's `private_key_jwt` credential at the fake Upstream IdP |

Regenerate with `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048`
(`openssl genrsa -traditional 2048` for the PKCS#1 key).

## Test-only TLS certificates

| File | Use |
|---|---|
| `tls-ca.pem` | a test CA; clients trust the test listeners through it |
| `tls-server.pem` | a server certificate from that CA for `localhost`, `127.0.0.1` and `::1` (P-256, valid to 2126) |
| `tls-server-key.pem` | its private key (PKCS#8) |

The CA's private key was discarded. To regenerate all three:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout ca-key.pem -out tls-ca.pem -days 36500 -subj "/CN=dbt-state test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout tls-server-key.pem -out server.csr -subj "/CN=localhost"
printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\n' > ext.cnf
openssl x509 -req -in server.csr -CA tls-ca.pem -CAkey ca-key.pem -out tls-server.pem \
  -days 36500 -extfile ext.cnf
rm ca-key.pem server.csr ext.cnf
```
