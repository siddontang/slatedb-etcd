//! Use fixed-width little-endian integers for native snapshots and lease records.

use bincode::config::legacy;
use bincode::error::{DecodeError, EncodeError};
use serde::{de::DeserializeOwned, Serialize};

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, EncodeError> {
    bincode::serde::encode_to_vec(value, legacy())
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    bincode::serde::decode_from_slice(bytes, legacy()).map(|(value, _)| value)
}

pub(crate) fn write(
    out: &mut impl std::io::Write,
    value: &impl Serialize,
) -> Result<usize, EncodeError> {
    bincode::serde::encode_into_std_write(value, out, legacy())
}

/// Decode one value from `input`, allocating at most `LIMIT` bytes for it.
pub(crate) fn read<T: DeserializeOwned, const LIMIT: usize>(
    input: &mut impl std::io::Read,
) -> Result<T, DecodeError> {
    bincode::serde::decode_from_std_read(input, legacy().with_limit::<LIMIT>())
}
