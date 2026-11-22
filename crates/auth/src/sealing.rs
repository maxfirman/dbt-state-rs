//! Encrypting the Upstream IdP refresh tokens the Token broker stores.
//!
//! Each value is sealed with AES-256-GCM (`ring::aead`) under the key from
//! `[upstream] refresh_token_key_file` and a fresh random 96-bit nonce. The
//! caller binds each value to its row with associated data (the person
//! session id), so a sealed value copied into another row does not open.
//!
//! Sealed layout: a version byte ([`VERSION`]), the nonce, then the
//! ciphertext and its tag. Rotating the key is out of scope (#215); the
//! version byte leaves room for it.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

/// The layout version of a sealed value.
const VERSION: u8 = 1;

/// Why a refresh-token key file was refused. Never carries its contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SealingKeyError {
    #[error("cannot read: {0}")]
    Unreadable(String),
    #[error("not the base64 of a 32-byte key (generate one with `openssl rand -base64 32`)")]
    Invalid,
}

/// The key the Token broker seals stored upstream refresh tokens with.
/// `Debug` never shows it.
#[derive(Clone)]
pub struct RefreshTokenKey(Arc<LessSafeKey>);

impl fmt::Debug for RefreshTokenKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RefreshTokenKey(<redacted>)")
    }
}

impl RefreshTokenKey {
    /// The key in `path`: the base64 of 32 random bytes, surrounding
    /// whitespace ignored.
    pub(crate) fn load(path: &Path) -> Result<Self, SealingKeyError> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| SealingKeyError::Unreadable(error.kind().to_string()))?;
        let bytes = STANDARD
            .decode(text.trim())
            .map_err(|_| SealingKeyError::Invalid)?;
        Self::from_bytes(&bytes).ok_or(SealingKeyError::Invalid)
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }
        let key = UnboundKey::new(&AES_256_GCM, bytes).ok()?;
        Some(Self(Arc::new(LessSafeKey::new(key))))
    }

    /// `plaintext` sealed, bound to `context`.
    pub fn seal(&self, plaintext: &[u8], context: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .expect("the system random source");
        let mut sealed = Vec::with_capacity(1 + NONCE_LEN + plaintext.len() + 16);
        sealed.push(VERSION);
        sealed.extend_from_slice(&nonce);
        let mut body = plaintext.to_vec();
        self.0
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(context),
                &mut body,
            )
            .expect("AES-256-GCM sealing");
        sealed.extend_from_slice(&body);
        sealed
    }

    /// The plaintext of `sealed`, if this key sealed it bound to `context`
    /// and it is unchanged.
    pub fn open(&self, sealed: &[u8], context: &[u8]) -> Option<Vec<u8>> {
        let (&version, rest) = sealed.split_first()?;
        if version != VERSION || rest.len() < NONCE_LEN {
            return None;
        }
        let (nonce, body) = rest.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
        let mut body = body.to_vec();
        let plaintext = self
            .0
            .open_in_place(nonce, Aad::from(context), &mut body)
            .ok()?;
        Some(plaintext.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> RefreshTokenKey {
        RefreshTokenKey::from_bytes(&[byte; 32]).unwrap()
    }

    #[test]
    fn a_sealed_value_opens_only_with_its_key_and_context() {
        let sealed = key(1).seal(b"upstream-refresh-token", b"session-a");
        assert!(!sealed
            .windows(b"upstream-refresh-token".len())
            .any(|w| w == b"upstream-refresh-token"));
        assert_eq!(
            key(1).open(&sealed, b"session-a").as_deref(),
            Some(&b"upstream-refresh-token"[..])
        );
        assert_eq!(key(1).open(&sealed, b"session-b"), None);
        assert_eq!(key(2).open(&sealed, b"session-a"), None);
        let mut tampered = sealed.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert_eq!(key(1).open(&tampered, b"session-a"), None);
        assert_eq!(key(1).open(&sealed[..10], b"session-a"), None);
    }

    #[test]
    fn each_seal_uses_a_fresh_nonce() {
        let key = key(3);
        assert_ne!(key.seal(b"same", b"ctx"), key.seal(b"same", b"ctx"));
    }
}
