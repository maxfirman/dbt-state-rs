//! RS256 keys: signing keys from mounted PEM files, and the public keys
//! tokens are checked against, published and read as a JSON Web Key Set.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256};
use rustls_pki_types::pem::{self, PemObject};
use rustls_pki_types::PrivateKeyDer;
use serde::{Deserialize, Serialize};

/// Why a signing key file was refused. Never carries the file's contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyFileError {
    #[error("cannot read: {0}")]
    Unreadable(String),
    #[error("no private key PEM section")]
    NoPrivateKey,
    #[error("invalid PEM")]
    InvalidPem,
    #[error("not an RSA private key")]
    NotRsa,
    #[error("unusable RSA private key ({0}); RS256 needs 2048 to 4096 bits")]
    Rejected(String),
}

/// One loaded signing key.
#[derive(Clone)]
pub(crate) struct SigningKey {
    pub(crate) id: String,
    pair: Arc<RsaKeyPair>,
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey").field("id", &self.id).finish()
    }
}

impl SigningKey {
    /// The RSA private key (PKCS#8 or PKCS#1 PEM) in `path`.
    pub(crate) fn load(id: String, path: &Path) -> Result<Self, KeyFileError> {
        let der = PrivateKeyDer::from_pem_file(path).map_err(|error| match error {
            pem::Error::Io(error) => KeyFileError::Unreadable(error.kind().to_string()),
            pem::Error::NoItemsFound => KeyFileError::NoPrivateKey,
            _ => KeyFileError::InvalidPem,
        })?;
        let pair = match &der {
            PrivateKeyDer::Pkcs8(der) => RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()),
            PrivateKeyDer::Pkcs1(der) => RsaKeyPair::from_der(der.secret_pkcs1_der()),
            _ => return Err(KeyFileError::NotRsa),
        }
        .map_err(|rejected| KeyFileError::Rejected(rejected.to_string()))?;
        Ok(Self {
            id,
            pair: Arc::new(pair),
        })
    }

    /// The RS256 signature of `message`.
    pub(crate) fn sign(&self, message: &[u8]) -> Vec<u8> {
        let mut signature = vec![0; self.pair.public().modulus_len()];
        self.pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                message,
                &mut signature,
            )
            .expect("RS256 signing with a loaded key");
        signature
    }

    fn public(&self) -> RsaPublicKeyComponents<Vec<u8>> {
        RsaPublicKeyComponents::from(self.pair.public())
    }
}

/// The loaded signing keys: several key ids, one active.
#[derive(Debug, Clone)]
pub(crate) struct SigningKeys {
    active: SigningKey,
    public: VerificationKeys,
}

impl SigningKeys {
    /// `keys`, signing with the one named `active_id`, which must be among
    /// them.
    pub(crate) fn new(keys: Vec<SigningKey>, active_id: &str) -> Option<Self> {
        let active = keys.iter().find(|key| key.id == active_id)?.clone();
        let public = PublicKeys(
            keys.iter()
                .map(|key| (key.id.clone(), key.public()))
                .collect(),
        );
        Some(Self {
            active,
            public: VerificationKeys::new(public),
        })
    }

    /// The key new tokens are signed with.
    pub(crate) fn active(&self) -> &SigningKey {
        &self.active
    }

    /// Every loaded key's public half.
    pub(crate) fn public(&self) -> &VerificationKeys {
        &self.public
    }
}

/// RS256 public keys by key id (`kid`).
#[derive(Default)]
pub(crate) struct PublicKeys(BTreeMap<String, RsaPublicKeyComponents<Vec<u8>>>);

impl fmt::Debug for PublicKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl PublicKeys {
    fn contains(&self, kid: &str) -> bool {
        self.0.contains_key(kid)
    }

    /// Whether `signature` is a valid RS256 signature of `message` by the key
    /// `kid`.
    fn verify(&self, kid: &str, message: &[u8], signature: &[u8]) -> bool {
        self.0.get(kid).is_some_and(|key| {
            key.verify(&RSA_PKCS1_2048_8192_SHA256, message, signature)
                .is_ok()
        })
    }
}

/// A JSON Web Key Set that held no usable RS256 key, or was not one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwksError {
    #[error("not a JSON Web Key Set")]
    Malformed,
    #[error("no RS256 signing key in the JSON Web Key Set")]
    NoKeys,
}

#[derive(Serialize, Deserialize)]
struct Jwks {
    keys: Vec<serde_json::Value>,
}

/// The members of an RSA JWK this crate reads or writes (RFC 7517, 7518).
#[derive(Serialize, Deserialize)]
struct RsaJwk {
    kty: String,
    #[serde(default, rename = "use", skip_serializing_if = "Option::is_none")]
    use_: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    alg: Option<String>,
    kid: String,
    n: String,
    e: String,
}

/// The public keys tokens are checked against: a shared handle, so a key
/// set fetched from a JWKS endpoint can be replaced while verifiers hold it.
#[derive(Clone)]
pub struct VerificationKeys(Arc<RwLock<Arc<PublicKeys>>>);

impl fmt::Debug for VerificationKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.current().fmt(f)
    }
}

impl VerificationKeys {
    fn new(keys: PublicKeys) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(keys))))
    }

    /// No keys yet: every signature fails until [`Self::replace_from_jwks`].
    pub fn empty() -> Self {
        Self::new(PublicKeys::default())
    }

    fn current(&self) -> Arc<PublicKeys> {
        Arc::clone(&self.0.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// Whether the set holds a key `kid`.
    pub(crate) fn contains(&self, kid: &str) -> bool {
        self.current().contains(kid)
    }

    /// Whether `signature` is a valid RS256 signature of `message` by the
    /// key `kid`.
    pub(crate) fn verify(&self, kid: &str, message: &[u8], signature: &[u8]) -> bool {
        self.current().verify(kid, message, signature)
    }

    /// Replace every key with the RS256 signing keys of the JSON Web Key Set
    /// `jwks`, returning how many there are. Keys for other algorithms or
    /// for encryption, and keys without a `kid`, are left out. On error the
    /// current keys stay.
    pub fn replace_from_jwks(&self, jwks: &[u8]) -> Result<usize, JwksError> {
        let set: Jwks = serde_json::from_slice(jwks).map_err(|_| JwksError::Malformed)?;
        let mut keys = BTreeMap::new();
        for key in set.keys {
            let Ok(key) = serde_json::from_value::<RsaJwk>(key) else {
                continue;
            };
            let usable = key.kty == "RSA"
                && key.use_.as_deref().is_none_or(|u| u == "sig")
                && key.alg.as_deref().is_none_or(|a| a == "RS256")
                && !key.kid.is_empty();
            let (Ok(n), Ok(e)) = (
                URL_SAFE_NO_PAD.decode(&key.n),
                URL_SAFE_NO_PAD.decode(&key.e),
            ) else {
                continue;
            };
            if usable {
                keys.insert(key.kid, RsaPublicKeyComponents { n, e });
            }
        }
        if keys.is_empty() {
            return Err(JwksError::NoKeys);
        }
        let count = keys.len();
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(PublicKeys(keys));
        Ok(count)
    }

    /// The keys as a JSON Web Key Set, for a JWKS endpoint.
    pub fn to_jwks(&self) -> String {
        let keys = self
            .current()
            .0
            .iter()
            .map(|(kid, key)| {
                serde_json::to_value(RsaJwk {
                    kty: "RSA".into(),
                    use_: Some("sig".into()),
                    alg: Some("RS256".into()),
                    kid: kid.clone(),
                    n: URL_SAFE_NO_PAD.encode(&key.n),
                    e: URL_SAFE_NO_PAD.encode(&key.e),
                })
                .expect("JWK JSON")
            })
            .collect();
        serde_json::to_string(&Jwks { keys }).expect("JWKS JSON")
    }
}
