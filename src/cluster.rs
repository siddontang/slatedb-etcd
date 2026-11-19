use crate::mvcc::KvError;
use crate::proto::etcdserverpb::*;
use raft::eraftpb::{ConfChange, ConfChangeType, ConfState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tonic::Code;

pub const KEY: &[u8] = b"etcd/members";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Store {
    pub members: BTreeMap<u64, Member>,
    pub removed: BTreeSet<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    List(MemberListRequest),
    Add(Member),
    Remove(u64),
    Update(MemberUpdateRequest),
    Promote(u64),
    Publish { id: u64, client_urls: Vec<String> },
}

impl Store {
    pub fn initial(peers: &BTreeMap<u64, String>) -> Self {
        Self {
            members: peers
                .iter()
                .map(|(&id, address)| {
                    (
                        id,
                        Member {
                            id,
                            name: format!("node-{id}"),
                            peer_ur_ls: vec![url(address)],
                            client_ur_ls: vec![url(address)],
                            is_learner: false,
                        },
                    )
                })
                .collect(),
            removed: BTreeSet::new(),
        }
    }

    pub fn conf(&self) -> ConfState {
        ConfState::from((
            self.members
                .values()
                .filter(|m| !m.is_learner)
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            self.members
                .values()
                .filter(|m| m.is_learner)
                .map(|m| m.id)
                .collect::<Vec<_>>(),
        ))
    }

    pub fn validate(&self, request: &Command) -> Result<(), KvError> {
        let missing = || KvError::new(Code::NotFound, "member not found");
        match request {
            Command::List(_) => (),
            Command::Publish { id, .. } => {
                self.members.get(id).ok_or_else(missing)?;
            }
            Command::Add(member) => {
                if self.members.contains_key(&member.id) || self.removed.contains(&member.id) {
                    return Err(KvError::new(Code::AlreadyExists, "member ID already exist"));
                }
                self.check_urls(member.id, &member.peer_ur_ls)?;
                if member.is_learner && self.members.values().any(|m| m.is_learner) {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "too many learner members in cluster",
                    ));
                }
            }
            Command::Remove(id) => {
                let member = self.members.get(id).ok_or_else(missing)?;
                if !member.is_learner
                    && self.members.values().filter(|m| !m.is_learner).count() == 1
                {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "re-configuration failed due to not enough started members",
                    ));
                }
            }
            Command::Update(r) => {
                self.members.get(&r.id).ok_or_else(missing)?;
                self.check_urls(r.id, &r.peer_ur_ls)?;
            }
            Command::Promote(id) => {
                if !self.members.get(id).ok_or_else(missing)?.is_learner {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "can only promote a learner member",
                    ));
                }
            }
        }
        Ok(())
    }

    fn check_urls(&self, id: u64, urls: &[String]) -> Result<(), KvError> {
        if urls.is_empty() {
            return Err(KvError::new(
                Code::InvalidArgument,
                "member requires a peer URL",
            ));
        }
        let mut unique = BTreeSet::new();
        for url in urls {
            let parsed = reqwest::Url::parse(url)
                .map_err(|_| KvError::new(Code::InvalidArgument, "invalid peer URL"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || parsed.port_or_known_default().is_none()
                || parsed.path() != "/"
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(KvError::new(Code::InvalidArgument, "invalid peer URL"));
            }
            if !unique.insert(url)
                || self
                    .members
                    .values()
                    .any(|m| m.id != id && m.peer_ur_ls.contains(url))
            {
                return Err(KvError::new(
                    Code::AlreadyExists,
                    "Peer URLs already exists",
                ));
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, request: &Command) {
        match request {
            Command::Add(member) => {
                self.members.insert(member.id, member.clone());
            }
            Command::Remove(id) => {
                self.members.remove(id);
                self.removed.insert(*id);
            }
            Command::Update(r) => {
                self.members.get_mut(&r.id).unwrap().peer_ur_ls = r.peer_ur_ls.clone();
            }
            Command::Promote(id) => {
                self.members.get_mut(id).unwrap().is_learner = false;
            }
            Command::Publish { id, client_urls } => {
                let member = self.members.get_mut(id).unwrap();
                member.name = format!("node-{id}");
                member.client_ur_ls = client_urls.clone();
            }
            Command::List(_) => (),
        }
    }
}

impl Command {
    pub fn change(&self) -> ConfChange {
        let (node_id, change_type) = match self {
            Self::Add(m) => (
                m.id,
                if m.is_learner {
                    ConfChangeType::AddLearnerNode
                } else {
                    ConfChangeType::AddNode
                },
            ),
            Self::Remove(id) => (*id, ConfChangeType::RemoveNode),
            Self::Promote(id) => (*id, ConfChangeType::AddNode),
            Self::Update(r) => (r.id, ConfChangeType::AddNode),
            Self::List(_) | Self::Publish { .. } => (0, ConfChangeType::AddNode),
        };
        ConfChange {
            node_id,
            change_type: change_type as i32,
            ..Default::default()
        }
    }
}

pub fn url(address: &str) -> String {
    if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}
