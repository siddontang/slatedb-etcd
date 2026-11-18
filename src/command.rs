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
        let payload = match inner {
            Self::Etcd { id, origin, etcd } => Payload::Kv(Kv {
                id: id.clone(),
                origin: *origin,
                request: Some(etcd.clone()),
                credentials: credentials.map(|auth| Credentials {
                    identity: auth.user.clone().map(Identity::User),
                    token: auth.token.clone(),
                    time: auth.time,
                }),
            }),
            _ => Payload::Json(serde_json::to_vec(self)?),
        };
        Ok(Envelope {
            payload: Some(payload),
        }
        .encode_to_vec())
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
