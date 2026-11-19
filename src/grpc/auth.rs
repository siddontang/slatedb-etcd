use super::*;
use crate::auth::Command;
use serde::de::DeserializeOwned;

impl Service {
    pub(super) async fn forward_unary<T, R>(
        &self,
        leader: u64,
        path: &'static str,
        body: T,
        credentials: &crate::auth::Credentials,
    ) -> Result<R, Status>
    where
        T: Message + Default + Send + Sync + 'static,
        R: Message + Default + Send + Sync + serde::Serialize + DeserializeOwned + 'static,
    {
        let channel = self.peer_channel(leader).await?;
        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let response: Response<R> = client
            .unary(
                forward_auth(body, credentials),
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic_prost::ProstCodec::default(),
            )
            .await?;
        if !path.starts_with("/etcdserverpb.Cluster/")
            && path != "/etcdserverpb.Maintenance/MoveLeader"
        {
            self.local_barrier().await?;
        }
        let mut value = serde_json::to_value(response.into_inner())
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(header) = value
            .get_mut("header")
            .and_then(serde_json::Value::as_object_mut)
        {
            header.insert("member_id".into(), serde_json::json!(self.member));
        }
        serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))
    }

    async fn auth_call<T, R>(
        &self,
        request: Request<T>,
        command: Command,
        path: &'static str,
    ) -> Result<Response<R>, Status>
    where
        T: Message + Default + Send + Sync + 'static,
        R: Message + Default + Send + Sync + serde::Serialize + DeserializeOwned + 'static,
    {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::Authenticated(
                credentials.clone(),
                Box::new(Event::Auth(command, tx)),
            ))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: auth request timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let response = match reply {
            Ok(value) => {
                let outcome: Outcome<serde_json::Value> =
                    serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
                let mut value = outcome.result.map_err(|e| e.status())?;
                value["header"]["member_id"] = serde_json::json!(self.member);
                serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?
            }
            Err(error) if error.error == "not leader" && !forwarded => {
                self.forward_unary(error.leader_id, path, request.into_inner(), &credentials)
                    .await?
            }
            Err(error) => return Err(Status::unavailable(error.error)),
        };
        Ok(Response::new(response))
    }
}

async fn hash_password(password: String) -> Result<String, Status> {
    tokio::task::spawn_blocking(move || bcrypt::hash(password, 10))
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::invalid_argument(e.to_string()))
}

#[tonic::async_trait]
impl auth_server::Auth for Service {
    async fn authenticate(
        &self,
        request: Request<AuthenticateRequest>,
    ) -> Result<Response<AuthenticateResponse>, Status> {
        self.local_barrier().await?;
        let (tx, rx) = oneshot::channel();
        self.events
            .send(Event::AuthLookup(request.get_ref().name.clone(), tx))
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let value = rx
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?
            .map_err(|e| Status::unavailable(e.error))?;
        let user: Option<crate::auth::User> =
            serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
        let error = || {
            Status::invalid_argument(
                "etcdserver: authentication failed, invalid user ID or password",
            )
        };
        let user = user.filter(|u| !u.no_password).ok_or_else(error)?;
        let hash = user.password.clone();
        let password = request.get_ref().password.clone();
        let verified = tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .unwrap_or(false);
        if !verified {
            return Err(error());
        }
        let (token, expires) = self
            .tokens
            .issue(&request.get_ref().name, credentials(&request).time)
            .map_err(|e| Status::internal(e.to_string()))?;
        let command = Command::IssueToken {
            name: request.get_ref().name.clone(),
            password: user.password,
            token,
            expires,
        };
        self.auth_call(request, command, "/etcdserverpb.Auth/Authenticate")
            .await
    }

    async fn user_add(
        &self,
        mut request: Request<AuthUserAddRequest>,
    ) -> Result<Response<AuthUserAddResponse>, Status> {
        let body = request.get_mut();
        if body.hashed_password.is_empty() && !body.options.as_ref().is_some_and(|o| o.no_password)
        {
            body.hashed_password = hash_password(std::mem::take(&mut body.password)).await?;
        }
        body.password.clear();
        let command = Command::UserAdd(body.clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserAdd")
            .await
    }

    async fn user_change_password(
        &self,
        mut request: Request<AuthUserChangePasswordRequest>,
    ) -> Result<Response<AuthUserChangePasswordResponse>, Status> {
        let body = request.get_mut();
        if body.hashed_password.is_empty() {
            body.hashed_password = hash_password(std::mem::take(&mut body.password)).await?;
        }
        body.password.clear();
        let command = Command::UserChangePassword(body.clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserChangePassword")
            .await
    }
    async fn auth_enable(
        &self,
        request: Request<AuthEnableRequest>,
    ) -> Result<Response<AuthEnableResponse>, Status> {
        let command = Command::Enable(*request.get_ref());
        self.auth_call(request, command, "/etcdserverpb.Auth/AuthEnable")
            .await
    }
    async fn auth_disable(
        &self,
        request: Request<AuthDisableRequest>,
    ) -> Result<Response<AuthDisableResponse>, Status> {
        let command = Command::Disable(*request.get_ref());
        self.auth_call(request, command, "/etcdserverpb.Auth/AuthDisable")
            .await
    }
    async fn auth_status(
        &self,
        request: Request<AuthStatusRequest>,
    ) -> Result<Response<AuthStatusResponse>, Status> {
        let command = Command::Status(*request.get_ref());
        self.auth_call(request, command, "/etcdserverpb.Auth/AuthStatus")
            .await
    }
    async fn user_get(
        &self,
        request: Request<AuthUserGetRequest>,
    ) -> Result<Response<AuthUserGetResponse>, Status> {
        let command = Command::UserGet(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserGet")
            .await
    }
    async fn user_list(
        &self,
        request: Request<AuthUserListRequest>,
    ) -> Result<Response<AuthUserListResponse>, Status> {
        let command = Command::UserList(*request.get_ref());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserList")
            .await
    }
    async fn user_delete(
        &self,
        request: Request<AuthUserDeleteRequest>,
    ) -> Result<Response<AuthUserDeleteResponse>, Status> {
        let command = Command::UserDelete(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserDelete")
            .await
    }
    async fn user_grant_role(
        &self,
        request: Request<AuthUserGrantRoleRequest>,
    ) -> Result<Response<AuthUserGrantRoleResponse>, Status> {
        let command = Command::UserGrantRole(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserGrantRole")
            .await
    }
    async fn user_revoke_role(
        &self,
        request: Request<AuthUserRevokeRoleRequest>,
    ) -> Result<Response<AuthUserRevokeRoleResponse>, Status> {
        let command = Command::UserRevokeRole(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/UserRevokeRole")
            .await
    }
    async fn role_add(
        &self,
        request: Request<AuthRoleAddRequest>,
    ) -> Result<Response<AuthRoleAddResponse>, Status> {
        let command = Command::RoleAdd(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleAdd")
            .await
    }
    async fn role_get(
        &self,
        request: Request<AuthRoleGetRequest>,
    ) -> Result<Response<AuthRoleGetResponse>, Status> {
        let command = Command::RoleGet(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleGet")
            .await
    }
    async fn role_list(
        &self,
        request: Request<AuthRoleListRequest>,
    ) -> Result<Response<AuthRoleListResponse>, Status> {
        let command = Command::RoleList(*request.get_ref());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleList")
            .await
    }
    async fn role_delete(
        &self,
        request: Request<AuthRoleDeleteRequest>,
    ) -> Result<Response<AuthRoleDeleteResponse>, Status> {
        let command = Command::RoleDelete(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleDelete")
            .await
    }
    async fn role_grant_permission(
        &self,
        request: Request<AuthRoleGrantPermissionRequest>,
    ) -> Result<Response<AuthRoleGrantPermissionResponse>, Status> {
        let command = Command::RoleGrantPermission(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleGrantPermission")
            .await
    }
    async fn role_revoke_permission(
        &self,
        request: Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<Response<AuthRoleRevokePermissionResponse>, Status> {
        let command = Command::RoleRevokePermission(request.get_ref().clone());
        self.auth_call(request, command, "/etcdserverpb.Auth/RoleRevokePermission")
            .await
    }
}
