//! Random bearer values the Token broker hands out (login `state`, nonces,
//! PKCE verifiers, authorization codes, refresh tokens) and the SHA-256
//! hashes it stores in their place.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};

/// 256 random bits, base64url without padding (43 characters).
pub(crate) fn random() -> String {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system random source");
    URL_SAFE_NO_PAD.encode(bytes)
}

/// The SHA-256 of `value`: what is stored for a bearer value.
pub(crate) fn hash(value: &str) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, value.as_bytes())
        .as_ref()
        .to_vec()
}

/// The PKCE S256 challenge of `verifier` (RFC 7636 §4.2).
pub(crate) fn s256_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(ring::digest::digest(
        &ring::digest::SHA256,
        verifier.as_bytes(),
    ))
}
