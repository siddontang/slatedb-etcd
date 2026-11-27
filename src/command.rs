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
                covers: 12,
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

    /// A seed keeps its membership and its digest.
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
    }

    /// An unknown command, an unknown field, or a field with a wrong type does not decode. Apply
    /// does not retry the error, so the node stops. See issue #110.
    #[test]
    fn an_unknown_or_malformed_command_does_not_decode() {
        let mut nested = serde_json::to_value(
            RaftCommand::CompactStep { id: "step".into() }
                .secure(Some(crate::auth::Credentials::default())),
        )
        .unwrap();
        nested["Secured"]["command"] = serde_json::json!({"Future": {"id": "step"}});
        for json in [
            // Untagged forms.
            serde_json::json!({"id": "step"}),
            serde_json::json!({"id": "lease", "origin": 3, "lease": {"action": "future"}}),
            serde_json::json!({"id": "compact", "compact": "wrong"}),
            serde_json::json!({"id": "future", "snapshot": "x"}),
            // Tagged forms.
            serde_json::json!({"Future": {"id": "step"}}),
            serde_json::json!({"Lease": {"id": "lease", "origin": 3, "lease": {"action": "x"}}}),
            serde_json::json!({"Compact": {"id": "compact", "compact": "wrong"}}),
            serde_json::json!({"CompactStep": {"id": "step", "snapshot": "x"}}),
            nested,
        ] {
            let data = Envelope {
                payload: Some(Payload::Json(serde_json::to_vec(&json).unwrap())),
            }
            .encode_to_vec();
            match RaftCommand::decode(&data) {
                Ok(command) => panic!("{json} decoded as {command:?}"),
                Err(error) => assert!(
                    crate::mvcc::storage_fault::classify(&error).is_none(),
                    "{error:#}"
                ),
            }
        }
    }

    /// An announcement keeps its coverage.
    #[test]
    fn a_view_checkpoint_decodes_its_coverage() {
        let command = RaftCommand::ViewCheckpoint {
            id: "view".into(),
            checkpoint: uuid::Uuid::new_v4().to_string(),
            covers: 9,
        };
        let decoded = RaftCommand::decode(&command.encode().unwrap()).unwrap();
        assert!(
            matches!(decoded, RaftCommand::ViewCheckpoint { covers: 9, .. }),
            "{decoded:?}"
        );
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
