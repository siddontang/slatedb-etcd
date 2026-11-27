//! Encode the HashKV rows: one row for each event under `HASH`, keyed by its revision and
//! subrevision.
//!
//! Put and Watch write these rows. HashKV and physical compaction read them.

use anyhow::Result;
use prost::Message;

use super::KvError;
use crate::maintenance::HASH;

pub(crate) fn hash_key(revision: i64, sub: i64, deleted: bool) -> Vec<u8> {
    let mut key = Vec::with_capacity(HASH.len() + 17 + usize::from(deleted));
    key.extend_from_slice(HASH);
    key.extend_from_slice(&revision.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(&sub.to_be_bytes());
    if deleted {
        key.push(b't');
    }
    key
}

// Hash records use the etcd KeyValue wire format. Retention only needs field one.
#[derive(prost::Message)]
struct HashUserKey {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
}

/// The first byte of a hash digest record.
///
/// A record is either an etcd `KeyValue` encoding (a tombstone) or a digest.
/// A protobuf field number is never zero, so no `KeyValue` encoding, now or after etcd adds
/// fields, starts with a zero byte. The marker does not depend on which fields exist.
const DIGEST: u8 = 0;
/// The digest layout after [`DIGEST`]: version, CRC-32C (big endian), length (big endian), key.
const DIGEST_V1: u8 = 1;
const DIGEST_HEADER: usize = 2 + 4 + 8;

/// Encode the hash record of a live `KeyValue`. `kv` is its etcd wire encoding.
///
/// HashKV feeds the record through [`hash_append`], which gives the same CRC as the full bytes.
/// The record does not repeat the value, so large values are not written a further time.
pub(crate) fn hash_record(key: &[u8], kv: &[u8]) -> Vec<u8> {
    let mut record = Vec::with_capacity(DIGEST_HEADER + key.len());
    record.extend_from_slice(&[DIGEST, DIGEST_V1]);
    record.extend_from_slice(&crc32c::crc32c(kv).to_be_bytes());
    record.extend_from_slice(&(kv.len() as u64).to_be_bytes());
    record.extend_from_slice(key);
    record
}

/// A decoded hash record.
enum HashRecord<'a> {
    /// The CRC-32C and length of a `KeyValue` encoding.
    Digest { key: Vec<u8>, crc: u32, len: u64 },
    /// The full bytes of a `KeyValue` encoding.
    Full(&'a [u8]),
}

fn parse_hash_record(record: &[u8]) -> Result<HashRecord<'_>> {
    if record.first() == Some(&DIGEST) {
        anyhow::ensure!(
            record.len() >= DIGEST_HEADER && record[1] == DIGEST_V1,
            "unsupported hash record"
        );
        return Ok(HashRecord::Digest {
            key: record[DIGEST_HEADER..].to_vec(),
            crc: u32::from_be_bytes(record[2..6].try_into()?),
            len: u64::from_be_bytes(record[6..14].try_into()?),
        });
    }
    Ok(HashRecord::Full(record))
}

/// Return the user key of a hash record.
pub(crate) fn hash_record_key(record: &[u8]) -> Result<Vec<u8>> {
    Ok(match parse_hash_record(record)? {
        HashRecord::Digest { key, .. } => key,
        HashRecord::Full(bytes) => HashUserKey::decode(bytes)?.key,
    })
}

/// Append the bytes that a hash record stands for to `crc`.
/// Returns the new CRC and the number of bytes that it added.
pub(crate) fn hash_append(crc: u32, record: &[u8]) -> Result<(u32, u64)> {
    Ok(match parse_hash_record(record)? {
        HashRecord::Digest { crc: part, len, .. } => {
            (crate::crc::combine(crc, part, usize::try_from(len)?), len)
        }
        HashRecord::Full(bytes) => (crc32c::crc32c_append(crc, bytes), bytes.len() as u64),
    })
}

pub(crate) fn hash_main(key: &[u8]) -> std::result::Result<i64, KvError> {
    // The whole key must have the form of `hash_key`: the main revision, `_`, the
    // subrevision, and an optional tombstone mark.
    let record = key.get(HASH.len()..).unwrap_or_default();
    let valid = match record.len() {
        17 => true,
        18 => record[17] == b't',
        _ => false,
    } && record[8] == b'_';
    match record.get(..8) {
        Some(main) if valid => Ok(i64::from_be_bytes(main.try_into().expect("eight bytes"))),
        _ => Err(KvError::corrupt("invalid hash key")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::mvccpb::KeyValue;

    #[test]
    fn a_digest_hashes_like_the_full_bytes() {
        let seed = crc32c::crc32c(b"seed");
        for (key, value) in [
            (b"k".to_vec(), vec![]),
            (b"key".to_vec(), vec![7; 300]),
            (vec![0xff; 200], vec![1; 70_000]),
        ] {
            let kv = KeyValue {
                key: key.clone(),
                create_revision: 2,
                mod_revision: 9,
                version: 3,
                value,
                lease: 5,
            }
            .encode_to_vec();
            let expected = (crc32c::crc32c_append(seed, &kv), kv.len() as u64);
            let digest = hash_record(&key, &kv);
            assert_eq!(digest[..2], [DIGEST, DIGEST_V1]);
            for record in [digest, kv.clone()] {
                assert_eq!(hash_append(seed, &record).unwrap(), expected);
                assert_eq!(hash_record_key(&record).unwrap(), key);
            }
        }
        // Tombstones keep the KeyValue bytes with only the key.
        let tombstone = KeyValue {
            key: b"gone".to_vec(),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(hash_record_key(&tombstone).unwrap(), b"gone");
        assert_eq!(
            hash_append(seed, &tombstone).unwrap(),
            (
                crc32c::crc32c_append(seed, &tombstone),
                tombstone.len() as u64
            )
        );
    }

    #[test]
    fn digests_do_not_depend_on_keyvalue_fields() {
        // A protobuf field number is never zero, so no KeyValue encoding starts with DIGEST.
        // This holds for fields that etcd may add later too, unlike a check for fields 14 and 15.
        for field in [1_u32, 5, 6, 7, 14, 15, 16, 1000, (1 << 29) - 1] {
            for wire in [0_u32, 1, 2, 5] {
                let tag = (field << 3) | wire;
                let mut encoded = vec![];
                prost::encoding::encode_varint(u64::from(tag), &mut encoded);
                assert_ne!(encoded[0], DIGEST, "field {field}, wire type {wire}");
            }
        }
        assert!(!KeyValue::default().encode_to_vec().starts_with(&[DIGEST]));
        // A later digest layout fails explicitly instead of being misread.
        let mut later = hash_record(b"k", b"kv");
        later[1] = DIGEST_V1 + 1;
        assert!(hash_append(0, &later).is_err());
        assert!(hash_record_key(&later).is_err());
        assert!(hash_append(0, &[DIGEST, DIGEST_V1, 0]).is_err());
    }
}
