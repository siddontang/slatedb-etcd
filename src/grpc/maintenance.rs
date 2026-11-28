use super::*;
use crate::maintenance::Command;
use serde_json::Value;
use tokio_stream::StreamExt;

/// How often a Defragment asks again while the new leader's writer opens.
const WRITER_RETRY: Duration = Duration::from_millis(100);

impl Service {
    async fn maintenance_call<T, R>(
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
        if !matches!(command, Command::Status) {
            self.local_barrier().await?;
        }
        // A shared-mode Defragment waits for its compacted checkpoint.
        let limit = Duration::from_secs(if matches!(command, Command::Defragment) {
            60
        } else {
            10
        });
        let deadline = tokio::time::Instant::now() + limit;
        let value = loop {
            let (tx, rx) = oneshot::channel();
            // The deadline covers the wait for room in the event queue too.
            let event = Event::Authenticated(
                credentials.clone(),
                Box::new(Event::Maintenance(command.clone(), tx)),
            );
            tokio::time::timeout_at(deadline, self.events.send(event))
                .await
                .map_err(|_| Status::deadline_exceeded("etcdserver: maintenance timed out"))?
                .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
            let value = tokio::time::timeout_at(deadline, rx)
                .await
                .map_err(|_| Status::deadline_exceeded("etcdserver: maintenance timed out"))?
                .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
            // A new leader opens its writer shortly after the election, and etcd clients do not
            // retry Defragment. Wait for the writer here, within the same deadline (#147).
            match &value {
                Err(error)
                    if error.error == crate::node::WRITER_NOT_OPEN
                        && tokio::time::Instant::now() + WRITER_RETRY < deadline =>
                {
                    tokio::time::sleep(WRITER_RETRY).await;
                }
                _ => break value,
            }
        };
        let response = match value {
            Ok(value) => {
                let value = serde_json::from_value::<Outcome<Value>>(value)
                    .map_err(|e| Status::internal(e.to_string()))?
                    .result
                    .map_err(|e| e.status())?;
                serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?
            }
            Err(error) if error.error == "not leader" && !forwarded => {
                // The forward gets what is left of the deadline, not a new one.
                let forward =
                    self.forward_unary(error.leader_id, path, request.into_inner(), &credentials);
                tokio::time::timeout_at(deadline, forward)
                    .await
                    .map_err(|_| Status::deadline_exceeded("etcdserver: maintenance timed out"))??
            }
            Err(error) => return Err(Status::unavailable(error.error)),
        };
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl maintenance_server::Maintenance for Service {
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.maintenance_call(request, Command::Status, "/etcdserverpb.Maintenance/Status")
            .await
    }
    async fn hash_kv(
        &self,
        request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        let command = Command::Hash(request.get_ref().revision);
        self.maintenance_call(request, command, "/etcdserverpb.Maintenance/HashKV")
            .await
    }
    async fn hash(&self, request: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        self.maintenance_call(request, Command::HashAll, "/etcdserverpb.Maintenance/Hash")
            .await
    }
    async fn defragment(
        &self,
        request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        self.maintenance_call(
            request,
            Command::Defragment,
            "/etcdserverpb.Maintenance/Defragment",
        )
        .await
    }
    async fn alarm(
        &self,
        mut request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        if request.get_ref().action == 1 && request.get_ref().member_id == 0 {
            request.get_mut().member_id = self.member;
        }
        let command = Command::Alarm(*request.get_ref());
        self.maintenance_call(request, command, "/etcdserverpb.Maintenance/Alarm")
            .await
    }
    async fn downgrade(
        &self,
        request: Request<DowngradeRequest>,
    ) -> Result<Response<DowngradeResponse>, Status> {
        let command = Command::Downgrade(request.get_ref().clone());
        self.maintenance_call(request, command, "/etcdserverpb.Maintenance/Downgrade")
            .await
    }
    async fn move_leader(
        &self,
        request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        let command = Command::MoveLeader(request.get_ref().target_id);
        self.maintenance_call(request, command, "/etcdserverpb.Maintenance/MoveLeader")
            .await
    }
    type SnapshotStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<SnapshotResponse, Status>> + Send + 'static>,
    >;
    /// Stream a native backup of this member's applied state. See `state::backup`.
    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let credentials = credentials(&request);
        self.local_barrier().await?;
        let (tx, rx) = oneshot::channel();
        self.events
            .send(Event::Authenticated(
                credentials,
                Box::new(Event::Snapshot(tx)),
            ))
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut backup = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: maintenance timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?
            .map_err(|error| error.status())?;
        let header = backup.header.take();
        let chunks = futures::stream::unfold(backup, |mut backup| async move {
            Some((backup.recv().await?, backup))
        });
        let stream = chunks.map(move |chunk| match chunk {
            Ok(chunk) => Ok(SnapshotResponse {
                header,
                remaining_bytes: chunk.remaining,
                blob: chunk.blob,
                version: crate::ETCD_API_VERSION.into(),
            }),
            Err(error) => {
                tracing::warn!(%error, "native backup stream failed");
                Err(crate::state::backup::backup_error(error).status())
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}
