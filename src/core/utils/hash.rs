//! Password hashing and cryptographic digest utilities.
//!
//! Password helpers use Argon2id with a fresh random salt for each new hash.
//! The SHA-256 submodule provides byte-oriented digest helpers.

mod argon;

pub mod sha256;

use crate::Result;

/// Verifies a plaintext password against an encoded Argon2 password hash.
///
/// Malformed hashes and password mismatches return an error. Success is
/// represented by `Ok(())`.
pub fn verify_password(password: &str, password_hash: &str) -> Result {
	argon::verify_password(password, password_hash)
}

/// Hashes a plaintext password with Argon2id.
///
/// A fresh random salt is generated for every call. The result is a
/// PHC-formatted string containing the salt and parameters needed for
/// verification.
pub fn password(password: &str) -> Result<String> { argon::password(password) }
