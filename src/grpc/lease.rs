use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use lease_client::LeaseClient;
use lease_server::Lease;
use tokio_stream::wrappers::ReceiverStream;

impl Service {
    async fn execute_lease(
        &self,
        command: LeaseCommand,
        forwarded: bool,
    ) -> Result<LeaseReply, Status> {
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::Lease(command.clone(), tx))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: request timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut response = match reply {
            Ok(value) => serde_json::from_value::<Outcome<LeaseReply>>(value)
                .map_err(|e| Status::internal(e.to_string()))?
                .result
                .map_err(|e| e.status())?,
            Err(error) if error.error == "not leader" && !forwarded => {
                let channel = self
                    .peers
                    .get(&error.leader_id)
                    .ok_or_else(|| Status::unavailable("etcdserver: no leader"))?;
                let mut client = LeaseClient::new(channel.clone());
                match command {
                    LeaseCommand::Grant { id, ttl } => LeaseReply::Grant(
                        client
                            .lease_grant(forward(LeaseGrantRequest { id, ttl }))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::Revoke { id } => LeaseReply::Revoke(
                        client
                            .lease_revoke(forward(LeaseRevokeRequest { id }))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::TimeToLive { id, keys } => LeaseReply::TimeToLive(
                        client
                            .lease_time_to_live(forward(LeaseTimeToLiveRequest { id, keys }))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::List => LeaseReply::List(
                        client
                            .lease_leases(forward(LeaseLeasesRequest {}))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::KeepAlive { id } => {
                        let request = forward(tokio_stream::iter([LeaseKeepAliveRequest { id }]));
                        let response = client
                            .lease_keep_alive(request)
                            .await?
                            .into_inner()
                            .message()
                            .await?
                            .ok_or_else(|| Status::unavailable("lease stream closed"))?;
                        LeaseReply::KeepAlive(response)
                    }
                    LeaseCommand::Expire { .. } => {
                        return Err(Status::internal("internal lease command"))
                    }
                }
            }
            Err(error) => return Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        };
        if let Some(header) = response.header_mut() {
            header.member_id = self.member;
        }
        Ok(response)
    }
}

#[tonic::async_trait]
impl Lease for Service {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let forwarded = is_forwarded(&request);
        let r = request.into_inner();
        match self
            .execute_lease(
                LeaseCommand::Grant {
                    id: r.id,
                    ttl: r.ttl,
                },
                forwarded,
            )
            .await?
        {
            LeaseReply::Grant(response) => Ok(Response::new(response)),
            _ => Err(Status::internal("unexpected lease response")),
        }
    }
    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let forwarded = is_forwarded(&request);
        match self
            .execute_lease(
                LeaseCommand::Revoke {
                    id: request.into_inner().id,
                },
                forwarded,
            )
            .await?
        {
            LeaseReply::Revoke(response) => Ok(Response::new(response)),
            _ => Err(Status::internal("unexpected lease response")),
        }
    }
    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let forwarded = is_forwarded(&request);
        let r = request.into_inner();
        match self
            .execute_lease(
                LeaseCommand::TimeToLive {
                    id: r.id,
                    keys: r.keys,
                },
                forwarded,
            )
            .await?
        {
            LeaseReply::TimeToLive(response) => Ok(Response::new(response)),
            _ => Err(Status::internal("unexpected lease response")),
        }
    }
    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        match self
            .execute_lease(LeaseCommand::List, is_forwarded(&request))
            .await?
        {
            LeaseReply::List(response) => Ok(Response::new(response)),
            _ => Err(Status::internal("unexpected lease response")),
        }
    }
    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, Status>>;
    async fn lease_keep_alive(
        &self,
        request: Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let forwarded = is_forwarded(&request);
        let mut input = request.into_inner();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            loop {
                let next =
                    tokio::select! { _ = tx.closed() => break, result = input.message() => result };
                let result = match next {
                    Ok(Some(request)) => match service
                        .execute_lease(LeaseCommand::KeepAlive { id: request.id }, forwarded)
                        .await
                    {
                        Ok(LeaseReply::KeepAlive(response)) => Ok(response),
                        Ok(_) => Err(Status::internal("unexpected lease response")),
                        Err(error) => Err(error),
                    },
                    Ok(None) => break,
                    Err(error) => Err(error),
                };
                let failed = result.is_err();
                if tx.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
