use super::*;
use crate::cluster::Command;
use serde_json::Value;

impl Service {
    async fn cluster_call<T, R>(
        &self,
        request: Request<T>,
        command: Command,
        path: &'static str,
    ) -> Result<Response<R>, Status>
    where
        T: Message + Default + Send + Sync + 'static,
        R: Message
            + Default
            + Send
            + Sync
            + serde::de::DeserializeOwned
            + serde::Serialize
            + 'static,
    {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        let (tx, rx) = oneshot::channel();
        self.events
            .send(Event::Authenticated(
                credentials.clone(),
                Box::new(Event::Cluster(command, tx)),
            ))
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: membership request timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let response = match reply {
            Ok(value) => {
                let outcome: Outcome<Value> =
                    serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
                serde_json::from_value(outcome.result.map_err(|e| e.status())?)
                    .map_err(|e| Status::internal(e.to_string()))?
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

#[tonic::async_trait]
impl cluster_server::Cluster for Service {
    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        if request.get_ref().linearizable {
            self.local_barrier().await?;
        }
        let command = Command::List(*request.get_ref());
        self.cluster_call(request, command, "/etcdserverpb.Cluster/MemberList")
            .await
    }
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        let command = Command::Add(Member {
            id: (uuid::Uuid::new_v4().as_u128() as u64).max(1),
            peer_ur_ls: request.get_ref().peer_ur_ls.clone(),
            is_learner: request.get_ref().is_learner,
            ..Default::default()
        });
        self.cluster_call(request, command, "/etcdserverpb.Cluster/MemberAdd")
            .await
    }
    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let command = Command::Remove(request.get_ref().id);
        self.cluster_call(request, command, "/etcdserverpb.Cluster/MemberRemove")
            .await
    }
    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        let command = Command::Update(request.get_ref().clone());
        self.cluster_call(request, command, "/etcdserverpb.Cluster/MemberUpdate")
            .await
    }
    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        let command = Command::Promote(request.get_ref().id);
        self.cluster_call(request, command, "/etcdserverpb.Cluster/MemberPromote")
            .await
    }
}
