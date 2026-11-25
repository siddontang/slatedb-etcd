use anyhow::{Context, Result};
use prost::Message;

use crate::proto::command::{credentials::Identity, envelope::Payload, Credentials, Envelope, Kv};
use crate::RaftCommand;

impl RaftCommand {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let (inner, credentials) = match self {
            Self::Secured {
                id,
                credentials,
                command,
            } if id == command.id() => (command.as_ref(), Some(credentials)),
            _ => (self, None),
        };
        if let Self::Etcd { id, origin, etcd } = inner {
            return Ok(Self::encode_kv(
                id.clone(),
                *origin,
                etcd.clone(),
                credentials.cloned(),
            ));
        }
        Ok(Envelope {
            payload: Some(Payload::Json(serde_json::to_vec(self)?)),
        }
        .encode_to_vec())
    }

    pub(crate) fn encode_kv(
        id: String,
        origin: u64,
        request: crate::proto::etcdserverpb::RequestOp,
        credentials: Option<crate::auth::Credentials>,
    ) -> Vec<u8> {
        let kv = Kv {
            id,
            origin,
            request: Some(request),
            credentials: credentials.map(|auth| Credentials {
                identity: auth.user.map(Identity::User),
                token: auth.token,
                time: auth.time,
            }),
        };
        Envelope {
            payload: Some(Payload::Kv(kv)),
        }
        .encode_to_vec()
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        match Envelope::decode(data)?
            .payload
            .context("empty Raft command")?
        {
            Payload::Kv(kv) => Ok(Self::Etcd {
                id: kv.id,
                origin: kv.origin,
                etcd: kv.request.context("missing KV request")?,
            }
            .secure(kv.credentials.map(|auth| crate::auth::Credentials {
                user: auth.identity.map(|Identity::User(user)| user),
                token: auth.token,
                time: auth.time,
            }))),
            Payload::Json(data) => Ok(serde_json::from_slice(&data)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::{request_op::Request, PutRequest, RequestOp};

    #[test]
    fn administrative_and_nested_commands_keep_their_payload() {
        let commands = [
            RaftCommand::Compact {
                id: "compact".into(),
                compact: crate::proto::etcdserverpb::CompactionRequest {
                    revision: 9,
                    physical: true,
                },
            },
            RaftCommand::ViewCheckpoint {
                id: "view".into(),
                checkpoint: uuid::Uuid::new_v4().to_string(),
            },
            RaftCommand::SetQuota {
                id: "quota".into(),
                bytes: -1,
                seed: true,
                requested_at: 0,
                member: 2,
            },
            seed_members(Some("0123456789abcdef".into())),
            RaftCommand::CompactStep { id: "step".into() },
            RaftCommand::Lease {
                id: "lease".into(),
                origin: 3,
                lease: crate::lease::LeaseCommand::Expire {
                    id: -7,
                    generation: 11,
                },
            },
        ];
        for command in commands {
            for command in [
                command.clone(),
                command
                    .secure(Some(crate::auth::Credentials::default()))
                    .secure(Some(crate::auth::Credentials::default())),
            ] {
                assert_eq!(
                    serde_json::to_value(RaftCommand::decode(&command.encode().unwrap()).unwrap())
                        .unwrap(),
                    serde_json::to_value(command).unwrap()
                );
            }
        }
    }

    fn seed_members(digest: Option<String>) -> RaftCommand {
        let peers = (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect();
        RaftCommand::SeedMembers(crate::cluster::Seed {
            id: "members".into(),
            origin: 2,
            members: serde_json::to_string(&crate::cluster::Store::initial(&peers)).unwrap(),
            digest,
            applied: 7,
        })
    }

    /// `CompactStep` matches any `id`, so `SeedMembers` must decode before it. An older binary
    /// fails to decode it.
    #[test]
    fn seed_members_does_not_decode_as_a_compaction_step() {
        for digest in [None, Some("0123456789abcdef".to_owned())] {
            let command = seed_members(digest.clone());
            let decoded = RaftCommand::decode(&command.encode().unwrap()).unwrap();
            let RaftCommand::SeedMembers(crate::cluster::Seed {
                id,
                origin,
                members,
                digest: decoded_digest,
                applied,
            }) = decoded
            else {
                panic!("{decoded:?}");
            };
            assert_eq!((id.as_str(), origin, applied), ("members", 2, 7));
            assert_eq!(decoded_digest, digest);
            let members: crate::cluster::Store = serde_json::from_str(&members).unwrap();
            assert_eq!(members.members.len(), 3);
        }
        #[derive(serde::Deserialize)]
        struct OlderCompactStep {
            #[allow(dead_code)]
            id: String,
        }
        let json = serde_json::to_vec(&seed_members(None)).unwrap();
        assert!(serde_json::from_slice::<OlderCompactStep>(&json).is_err());
    }

    /// `CompactStep` matches any `id`, so `SetQuota` must decode before it.
    #[test]
    fn set_quota_does_not_decode_as_a_compaction_step() {
        for (bytes, seed, at) in [(2 << 30, true, 0), (0, false, 7), (-3, false, u64::MAX)] {
            let command = RaftCommand::SetQuota {
                id: "quota".into(),
                bytes,
                seed,
                requested_at: at,
                member: 3,
            };
            let decoded = RaftCommand::decode(&command.encode().unwrap()).unwrap();
            assert!(
                matches!(
                    decoded,
                    RaftCommand::SetQuota { ref id, bytes: b, seed: s, requested_at: t, member: 3 }
                        if id == "quota" && b == bytes && s == seed && t == at
                ),
                "{decoded:?}"
            );
        }
        let step = RaftCommand::CompactStep { id: "step".into() };
        let step = RaftCommand::decode(&step.encode().unwrap()).unwrap();
        assert!(matches!(step, RaftCommand::CompactStep { .. }));
        // An older binary has no `SetQuota`. Its `CompactStep` needs an `id`, so it rejects the
        // entry and does not apply a compaction step that other members skip.
        #[derive(serde::Deserialize)]
        struct OlderCompactStep {
            #[allow(dead_code)]
            id: String,
        }
        let json = serde_json::to_vec(&RaftCommand::SetQuota {
            id: "quota".into(),
            bytes: 1,
            seed: false,
            requested_at: 1,
            member: 1,
        })
        .unwrap();
        assert!(serde_json::from_slice::<OlderCompactStep>(&json).is_err());
    }

    #[test]
    fn compact_kv_log_preserves_binary_values_and_credentials() {
        let command = RaftCommand::Etcd {
            id: "request".into(),
            origin: 17,
            etcd: RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: vec![0, 255],
                    value: (0..=255).collect(),
                    prev_kv: true,
                    ..Default::default()
                })),
            },
        };
        for auth in [
            None,
            Some(crate::auth::Credentials::default()),
            Some(crate::auth::Credentials {
                user: Some(String::new()),
                token: "token".into(),
                time: 42,
            }),
        ] {
            let command = command.clone().secure(auth);
            let encoded = command.encode().unwrap();
            assert!(encoded.len() < serde_json::to_vec(&command).unwrap().len() / 2);
            assert_eq!(
                serde_json::to_value(RaftCommand::decode(&encoded).unwrap()).unwrap(),
                serde_json::to_value(&command).unwrap()
            );
            assert!(RaftCommand::decode(&encoded[..encoded.len() - 1]).is_err());
        }
        assert!(RaftCommand::decode(&[]).is_err());
        assert!(RaftCommand::decode(
            &Envelope {
                payload: Some(Payload::Kv(Kv::default()))
            }
            .encode_to_vec()
        )
        .is_err());
    }
}
