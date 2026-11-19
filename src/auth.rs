use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Code;

use crate::mvcc::KvError;
use crate::proto::authpb::Permission;
use crate::proto::etcdserverpb::*;

pub const KEY: &[u8] = b"etcd/auth";
type Result<T> = std::result::Result<T, KvError>;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct Credentials {
    pub user: Option<String>,
    pub token: String,
    pub time: u64,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct User {
    pub password: String,
    pub no_password: bool,
    pub roles: BTreeSet<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Token {
    user: String,
    revision: u64,
    expires: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Store {
    pub enabled: bool,
    pub revision: u64,
    pub users: BTreeMap<String, User>,
    roles: BTreeMap<String, Vec<Permission>>,
    tokens: BTreeMap<String, Token>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            enabled: false,
            revision: 1,
            users: BTreeMap::new(),
            roles: BTreeMap::new(),
            tokens: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Command {
    Enable(AuthEnableRequest),
    Disable(AuthDisableRequest),
    Status(AuthStatusRequest),
    UserAdd(AuthUserAddRequest),
    UserGet(AuthUserGetRequest),
    UserList(AuthUserListRequest),
    UserDelete(AuthUserDeleteRequest),
    UserChangePassword(AuthUserChangePasswordRequest),
    UserGrantRole(AuthUserGrantRoleRequest),
    UserRevokeRole(AuthUserRevokeRoleRequest),
    RoleAdd(AuthRoleAddRequest),
    RoleGet(AuthRoleGetRequest),
    RoleList(AuthRoleListRequest),
    RoleDelete(AuthRoleDeleteRequest),
    RoleGrantPermission(AuthRoleGrantPermissionRequest),
    RoleRevokePermission(AuthRoleRevokePermissionRequest),
    IssueToken {
        name: String,
        password: String,
        token: String,
        expires: u64,
    },
}

pub fn denied() -> KvError {
    KvError::new(Code::PermissionDenied, "permission denied")
}

fn invalid(message: &str) -> KvError {
    KvError::new(Code::InvalidArgument, message)
}

impl Store {
    pub(crate) fn reset_tokens(&mut self) {
        self.tokens.clear();
        self.revision += 1;
    }

    pub fn identity<'a>(&'a self, auth: &'a Credentials) -> Result<&'a str> {
        if let Some(user) = &auth.user {
            if self.users.contains_key(user) {
                return Ok(user);
            }
            return Err(denied());
        }
        if auth.token.is_empty() {
            return Err(invalid("user name is empty"));
        }
        let token = self
            .tokens
            .get(&auth.token)
            .filter(|t| t.expires > auth.time)
            .ok_or_else(|| KvError::new(Code::Unauthenticated, "invalid auth token"))?;
        if token.revision != self.revision || !self.users.contains_key(&token.user) {
            return Err(KvError::new(
                Code::InvalidArgument,
                "revision of auth store is old",
            ));
        }
        Ok(&token.user)
    }

    fn is_root(&self, name: &str) -> bool {
        self.users
            .get(name)
            .is_some_and(|u| u.roles.contains("root"))
    }

    pub fn admin(&self, auth: &Credentials) -> Result<()> {
        if !self.enabled || self.is_root(self.identity(auth)?) {
            Ok(())
        } else {
            Err(denied())
        }
    }

    pub fn authenticated(&self, auth: &Credentials) -> Result<()> {
        if self.enabled {
            self.identity(auth)?;
        }
        Ok(())
    }

    pub fn range(&self, auth: &Credentials, key: &[u8], end: &[u8], write: bool) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let name = self.identity(auth)?;
        if self.is_root(name) {
            return Ok(());
        }
        let user = &self.users[name];
        let mut ranges: Vec<_> = user
            .roles
            .iter()
            .filter_map(|r| self.roles.get(r))
            .flatten()
            .filter(|p| p.perm_type == 2 || p.perm_type == i32::from(write))
            .collect();
        ranges.sort_by(|a, b| a.key.cmp(&b.key));
        if end.is_empty() {
            if ranges
                .iter()
                .any(|p| crate::mvcc::contains(key, &p.key, &p.range_end))
            {
                return Ok(());
            }
        } else {
            // A point covers [key, key + NUL). Merge adjacent grants before checking the request.
            let mut covered = key.to_vec();
            for p in ranges {
                if p.key > covered {
                    continue;
                }
                if p.range_end == [0] {
                    return Ok(());
                }
                let grant_end = if p.range_end.is_empty() {
                    [p.key.as_slice(), &[0]].concat()
                } else {
                    p.range_end.clone()
                };
                if grant_end > covered {
                    covered = grant_end;
                }
                if end != [0] && covered.as_slice() >= end {
                    return Ok(());
                }
            }
        }
        Err(denied())
    }

    pub fn kv(&self, auth: &Credentials, request: &RequestOp) -> Result<()> {
        use request_op::Request as Op;
        match &request.request {
            Some(Op::RequestRange(r)) => self.range(auth, &r.key, &r.range_end, false),
            Some(Op::RequestPut(r)) => {
                self.range(auth, &r.key, &[], true)?;
                if r.prev_kv {
                    self.range(auth, &r.key, &[], false)?;
                }
                Ok(())
            }
            Some(Op::RequestDeleteRange(r)) => {
                self.range(auth, &r.key, &r.range_end, true)?;
                if r.prev_kv {
                    self.range(auth, &r.key, &r.range_end, false)?;
                }
                Ok(())
            }
            Some(Op::RequestTxn(r)) => {
                for c in &r.compare {
                    self.range(auth, &c.key, &c.range_end, false)?;
                }
                for op in r.success.iter().chain(&r.failure) {
                    self.kv(auth, op)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn execute(
        &mut self,
        auth: &Credentials,
        command: &Command,
        revision: i64,
    ) -> Result<Value> {
        let name = if self.enabled && !matches!(command, Command::IssueToken { .. }) {
            self.identity(auth)?.to_owned()
        } else {
            String::new()
        };
        let root = !self.enabled || self.is_root(&name);
        let allowed = match command {
            Command::IssueToken { .. } => true,
            Command::UserGet(r) => root || r.name == name,
            Command::RoleGet(r) => {
                root || self
                    .users
                    .get(&name)
                    .is_some_and(|u| u.roles.contains(&r.role))
            }
            Command::RoleList(_) => true,
            _ => root,
        };
        if !allowed {
            return Err(denied());
        }
        let mut changed = true;
        let mut response = json!({"header": crate::mvcc::header(revision)});
        match command {
            Command::Enable(_) => {
                if !self.users.contains_key("root") {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "root user does not exist",
                    ));
                }
                if !self.is_root("root") {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "root user does not have root role",
                    ));
                }
                changed = !self.enabled;
                self.enabled = true;
            }
            Command::Disable(_) => {
                changed = self.enabled;
                self.enabled = false;
            }
            Command::Status(_) => {
                changed = false;
                response["enabled"] = json!(self.enabled);
                response["auth_revision"] = json!(self.revision);
            }
            Command::UserAdd(r) => {
                if r.name.is_empty() {
                    return Err(invalid("user name is empty"));
                }
                if self.users.contains_key(&r.name) {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "user name already exists",
                    ));
                }
                self.users.insert(
                    r.name.clone(),
                    User {
                        password: r.hashed_password.clone(),
                        no_password: r.options.as_ref().is_some_and(|o| o.no_password),
                        roles: BTreeSet::new(),
                    },
                );
            }
            Command::UserGet(r) => {
                changed = false;
                response["roles"] = json!(self.user(&r.name)?.roles);
            }
            Command::UserList(_) => {
                changed = false;
                response["users"] = json!(self.users.keys().collect::<Vec<_>>());
            }
            Command::UserDelete(r) => {
                self.protect_root(&r.name)?;
                self.user(&r.name)?;
                self.users.remove(&r.name);
            }
            Command::UserChangePassword(r) => {
                self.user(&r.name)?;
                let user = self.users.get_mut(&r.name).unwrap();
                user.password = r.hashed_password.clone();
                user.no_password = false;
            }
            Command::UserGrantRole(r) => {
                self.user(&r.user)?;
                if r.role != "root" {
                    self.role(&r.role)?;
                }
                self.users
                    .get_mut(&r.user)
                    .unwrap()
                    .roles
                    .insert(r.role.clone());
            }
            Command::UserRevokeRole(r) => {
                if r.name == "root" && r.role == "root" {
                    self.protect_root("root")?;
                }
                self.user(&r.name)?;
                if !self.users.get_mut(&r.name).unwrap().roles.remove(&r.role) {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "role is not granted to the user",
                    ));
                }
            }
            Command::RoleAdd(r) => {
                if r.name.is_empty() {
                    return Err(invalid("role name is empty"));
                }
                if self.roles.contains_key(&r.name) {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "role name already exists",
                    ));
                }
                self.roles.insert(r.name.clone(), vec![]);
            }
            Command::RoleGet(r) => {
                changed = false;
                response["perm"] = if r.role == "root" {
                    json!([Permission {
                        perm_type: 2,
                        key: vec![0],
                        range_end: vec![0]
                    }])
                } else {
                    json!(self.role(&r.role)?)
                };
            }
            Command::RoleList(_) => {
                changed = false;
                response["roles"] = if root {
                    json!(self.roles.keys().collect::<Vec<_>>())
                } else {
                    json!(self.user(&name)?.roles)
                };
            }
            Command::RoleDelete(r) => {
                self.protect_root(&r.role)?;
                self.role(&r.role)?;
                self.roles.remove(&r.role);
                for u in self.users.values_mut() {
                    u.roles.remove(&r.role);
                }
            }
            Command::RoleGrantPermission(r) => {
                self.role(&r.name)?;
                let p = r
                    .perm
                    .as_ref()
                    .ok_or_else(|| invalid("permission not given"))?;
                if !(0..=2).contains(&p.perm_type) {
                    return Err(invalid("invalid permission type"));
                }
                let permissions = self.roles.get_mut(&r.name).unwrap();
                permissions.retain(|old| old.key != p.key || old.range_end != p.range_end);
                permissions.push(p.clone());
                permissions.sort_by(|a, b| (&a.key, &a.range_end).cmp(&(&b.key, &b.range_end)));
            }
            Command::RoleRevokePermission(r) => {
                self.role(&r.role)?;
                let permissions = self.roles.get_mut(&r.role).unwrap();
                let before = permissions.len();
                permissions.retain(|p| p.key != r.key || p.range_end != r.range_end);
                if before == permissions.len() {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "permission is not granted to the role",
                    ));
                }
            }
            Command::IssueToken {
                name,
                password,
                token,
                expires,
            } => {
                changed = false;
                if !self.enabled {
                    return Err(KvError::new(
                        Code::FailedPrecondition,
                        "authentication is not enabled",
                    ));
                }
                let user = self
                    .users
                    .get(name)
                    .filter(|u| !u.no_password && u.password == *password)
                    .ok_or_else(|| {
                        KvError::new(
                            Code::InvalidArgument,
                            "authentication failed, invalid user ID or password",
                        )
                    })?;
                let _ = user;
                self.tokens
                    .retain(|_, t| t.expires > auth.time && t.revision == self.revision);
                if self.tokens.len() >= 10_000 {
                    return Err(KvError::new(
                        Code::ResourceExhausted,
                        "too many auth tokens",
                    ));
                }
                self.tokens.insert(
                    token.clone(),
                    Token {
                        user: name.clone(),
                        revision: self.revision,
                        expires: *expires,
                    },
                );
                response["token"] = json!(token);
            }
        }
        if changed {
            self.revision += 1;
            self.tokens.clear();
        }
        Ok(response)
    }

    fn protect_root(&self, name: &str) -> Result<()> {
        if self.enabled && name == "root" {
            Err(invalid("invalid auth management"))
        } else {
            Ok(())
        }
    }

    fn user(&self, name: &str) -> Result<&User> {
        self.users
            .get(name)
            .ok_or_else(|| KvError::new(Code::FailedPrecondition, "user name not found"))
    }

    fn role(&self, name: &str) -> Result<&Vec<Permission>> {
        self.roles
            .get(name)
            .ok_or_else(|| KvError::new(Code::FailedPrecondition, "role name not found"))
    }
}

#[derive(Clone, Default)]
pub struct TokenIssuer {
    key: Option<std::sync::Arc<jsonwebtoken::EncodingKey>>,
    ttl: u64,
}

impl TokenIssuer {
    pub fn parse(config: &str) -> anyhow::Result<Self> {
        if config.is_empty() || config == "simple" {
            return Ok(Self {
                key: None,
                ttl: 300,
            });
        }
        let mut parts = config.split(',');
        anyhow::ensure!(parts.next() == Some("jwt"), "invalid auth token type");
        let mut options = BTreeMap::new();
        for part in parts {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid JWT option"))?;
            anyhow::ensure!(options.insert(key, value).is_none(), "duplicate JWT option");
        }
        anyhow::ensure!(
            options.get("sign-method") == Some(&"RS256"),
            "JWT requires RS256"
        );
        let path = options
            .get("priv-key")
            .ok_or_else(|| anyhow::anyhow!("JWT requires a private key"))?;
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(&std::fs::read(path)?)?;
        let public = options
            .get("pub-key")
            .ok_or_else(|| anyhow::anyhow!("JWT requires a public key"))?;
        let public = jsonwebtoken::DecodingKey::from_rsa_pem(&std::fs::read(public)?)?;
        let ttl = options.get("ttl").copied().unwrap_or("300s");
        let ttl: u64 = ttl
            .strip_suffix('s')
            .ok_or_else(|| anyhow::anyhow!("JWT TTL must use seconds"))?
            .parse()?;
        anyhow::ensure!(ttl > 0, "JWT TTL must be positive");
        let issuer = Self {
            key: Some(std::sync::Arc::new(key)),
            ttl,
        };
        let (token, _) = issuer.issue("key-check", 4_000_000_000)?;
        jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &public,
            &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256),
        )?;
        Ok(issuer)
    }

    pub fn issue(&self, name: &str, now: u64) -> anyhow::Result<(String, u64)> {
        let expires = now + self.ttl.max(1);
        let token = if let Some(key) = &self.key {
            jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                &json!({"username": name, "exp": expires, "iat": now,
                    "jti": uuid::Uuid::new_v4().to_string()}),
                key,
            )?
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        Ok((token, expires))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_and_adjacent_grants_cover_only_the_authorized_range() {
        let mut store = Store {
            enabled: true,
            ..Store::default()
        };
        store.users.insert(
            "writer".into(),
            User {
                roles: BTreeSet::from(["points".into()]),
                ..User::default()
            },
        );
        store.roles.insert(
            "points".into(),
            vec![Permission {
                perm_type: 2,
                key: b"point".to_vec(),
                range_end: vec![],
            }],
        );
        let credentials = Credentials {
            user: Some("writer".into()),
            ..Credentials::default()
        };
        assert!(store
            .range(&credentials, b"point", b"point\0", false)
            .is_ok());
        assert!(store
            .range(&credentials, b"point", b"point\x01", false)
            .is_err());
        store.roles.get_mut("points").unwrap().push(Permission {
            perm_type: 2,
            key: b"point\0".to_vec(),
            range_end: b"q".to_vec(),
        });
        assert!(store.range(&credentials, b"point", b"q", true).is_ok());
        assert!(store.range(&credentials, b"point", b"r", true).is_err());
        assert!(store.range(&credentials, b"point", &[0], true).is_err());
    }
}
