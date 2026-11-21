//! RS256 signing keys from mounted PEM files.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256};
use rustls_pki_types::pem::{self, PemObject};
use rustls_pki_types::PrivateKeyDer;

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

    /// The public half (DER `RSAPublicKey`).
    fn public_der(&self) -> Vec<u8> {
        self.pair.public().as_ref().to_vec()
    }
}

/// The loaded signing keys: several key ids, one active.
#[derive(Debug, Clone)]
pub(crate) struct SigningKeys {
    active: SigningKey,
    public: Arc<PublicKeys>,
}

impl SigningKeys {
    /// `keys`, signing with the one named `active_id`, which must be among
    /// them.
    pub(crate) fn new(keys: Vec<SigningKey>, active_id: &str) -> Option<Self> {
        let active = keys.iter().find(|key| key.id == active_id)?.clone();
        let public = PublicKeys(
            keys.iter()
                .map(|key| {
                    (
                        key.id.clone(),
                        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key.public_der()),
                    )
                })
                .collect(),
        );
        Some(Self {
            active,
            public: Arc::new(public),
        })
    }

    /// The key new tokens are signed with.
    pub(crate) fn active(&self) -> &SigningKey {
        &self.active
    }

    /// Every loaded key's public half, by key id.
    pub(crate) fn public(&self) -> Arc<PublicKeys> {
        Arc::clone(&self.public)
    }
}

/// RS256 public keys by key id (`kid`).
pub(crate) struct PublicKeys(BTreeMap<String, UnparsedPublicKey<Vec<u8>>>);

impl fmt::Debug for PublicKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl PublicKeys {
    /// Whether `signature` is a valid RS256 signature of `message` by the key
    /// `kid`.
    pub(crate) fn verify(&self, kid: &str, message: &[u8], signature: &[u8]) -> bool {
        self.0
            .get(kid)
            .is_some_and(|key| key.verify(message, signature).is_ok())
    }
}
