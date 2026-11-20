use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use lease_client::LeaseClient;
use lease_server::Lease;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio_stream::Stream;

type KeepAliveReply = Result<Outcome<LeaseReply>, crate::ApiError>;
type KeepAliveResult = Result<LeaseKeepAliveResponse, Status>;

impl Service {
    async fn execute_lease(
        &self,
        command: LeaseCommand,
        forwarded: bool,
        credentials: crate::auth::Credentials,
    ) -> Result<LeaseReply, Status> {
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::Authenticated(
                credentials.clone(),
                Box::new(Event::Lease(command.clone(), tx)),
            ))
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
                self.forward_lease(error.leader_id, command, &credentials)
                    .await?
            }
            Err(error) => return Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        };
        if let Some(header) = response.header_mut() {
            header.member_id = self.member;
        }
        Ok(response)
    }

    /// Send a lease request to the leader, then wait until this member applies its result.
    async fn forward_lease(
        &self,
        leader: u64,
        command: LeaseCommand,
        credentials: &crate::auth::Credentials,
    ) -> Result<LeaseReply, Status> {
        let channel = self.peer_channel(leader).await?;
        let mut client = LeaseClient::new(channel);
        let response = self.forwarded(
            leader,
            async {
                Ok(match command {
                    LeaseCommand::Grant { id, ttl } => LeaseReply::Grant(
                        client
                            .lease_grant(forward_auth(LeaseGrantRequest { id, ttl }, credentials))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::Revoke { id } => LeaseReply::Revoke(
                        client
                            .lease_revoke(forward_auth(LeaseRevokeRequest { id }, credentials))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::TimeToLive { id, keys } => LeaseReply::TimeToLive(
                        client
                            .lease_time_to_live(forward_auth(
                                LeaseTimeToLiveRequest { id, keys },
                                credentials,
                            ))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::List => LeaseReply::List(
                        client
                            .lease_leases(forward_auth(LeaseLeasesRequest {}, credentials))
                            .await?
                            .into_inner(),
                    ),
                    LeaseCommand::KeepAlive { id } => {
                        let request = forward_auth(
                            tokio_stream::iter([LeaseKeepAliveRequest { id }]),
                            credentials,
                        );
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
                })
            }
            .await,
        )?;
        self.local_barrier().await?;
        Ok(response)
    }

    /// Renew one lease. The typed reply skips the JSON round trip of other lease requests.
    fn send_keep_alive(
        &self,
        id: i64,
        credentials: &crate::auth::Credentials,
    ) -> Result<oneshot::Receiver<KeepAliveReply>, Status> {
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::KeepAlive {
                id,
                credentials: credentials.clone(),
                response: tx,
            })
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        Ok(rx)
    }
}

#[tonic::async_trait]
impl Lease for Service {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        let r = request.into_inner();
        match self
            .execute_lease(
                LeaseCommand::Grant {
                    id: r.id,
                    ttl: r.ttl,
                },
                forwarded,
                credentials,
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
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        match self
            .execute_lease(
                LeaseCommand::Revoke {
                    id: request.into_inner().id,
                },
                forwarded,
                credentials,
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
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        let r = request.into_inner();
        match self
            .execute_lease(
                LeaseCommand::TimeToLive {
                    id: r.id,
                    keys: r.keys,
                },
                forwarded,
                credentials,
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
            .execute_lease(
                LeaseCommand::List,
                is_forwarded(&request),
                credentials(&request),
            )
            .await?
        {
            LeaseReply::List(response) => Ok(Response::new(response)),
            _ => Err(Status::internal("unexpected lease response")),
        }
    }
    type LeaseKeepAliveStream = KeepAliveStream;
    async fn lease_keep_alive(
        &self,
        request: Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        Ok(Response::new(KeepAliveStream {
            service: self.clone(),
            input: request.into_inner(),
            credentials,
            forwarded,
            timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),
            state: KeepAliveState::Idle,
        }))
    }
}

/// Serve one KeepAlive stream inside the response body, without a task or channel per stream.
///
/// Requests on one stream are served in order, one at a time, as before. The stream ends after
/// the first error. Dropping the response drops the pending reply, so the node skips the renewal.
pub struct KeepAliveStream {
    service: Service,
    input: tonic::Streaming<LeaseKeepAliveRequest>,
    credentials: crate::auth::Credentials,
    forwarded: bool,
    /// Reused for each request on the stream.
    timeout: Pin<Box<tokio::time::Sleep>>,
    state: KeepAliveState,
}

enum KeepAliveState {
    Idle,
    Local {
        id: i64,
        reply: oneshot::Receiver<KeepAliveReply>,
    },
    Forward(Pin<Box<dyn Future<Output = KeepAliveResult> + Send>>),
    Done,
}

impl KeepAliveStream {
    /// Return one result. An error ends the stream.
    fn finish(&mut self, result: KeepAliveResult) -> Poll<Option<KeepAliveResult>> {
        self.state = match &result {
            Ok(_) => KeepAliveState::Idle,
            Err(_) => KeepAliveState::Done,
        };
        Poll::Ready(Some(result.map(|mut response| {
            if let Some(header) = response.header.as_mut() {
                header.member_id = self.service.member;
            }
            response
        })))
    }

    /// Forward a request to the leader when this member is a follower.
    fn forward(&mut self, id: i64, leader: u64) {
        let service = self.service.clone();
        let credentials = self.credentials.clone();
        self.state = KeepAliveState::Forward(Box::pin(async move {
            match service
                .forward_lease(leader, LeaseCommand::KeepAlive { id }, &credentials)
                .await?
            {
                LeaseReply::KeepAlive(response) => Ok(response),
                _ => Err(Status::internal("unexpected lease response")),
            }
        }));
    }
}

impl Stream for KeepAliveStream {
    type Item = KeepAliveResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                KeepAliveState::Done => return Poll::Ready(None),
                KeepAliveState::Idle => {
                    let id = match ready!(Pin::new(&mut this.input).poll_next(cx)) {
                        None => {
                            this.state = KeepAliveState::Done;
                            return Poll::Ready(None);
                        }
                        Some(Err(error)) => return this.finish(Err(error)),
                        Some(Ok(request)) => request.id,
                    };
                    match this.service.send_keep_alive(id, &this.credentials) {
                        Ok(reply) => {
                            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                            this.timeout.as_mut().reset(deadline);
                            this.state = KeepAliveState::Local { id, reply };
                        }
                        Err(error) => return this.finish(Err(error)),
                    }
                }
                KeepAliveState::Local { id, reply } => {
                    let id = *id;
                    let reply = match Pin::new(reply).poll(cx) {
                        Poll::Ready(reply) => reply,
                        Poll::Pending => {
                            ready!(this.timeout.as_mut().poll(cx));
                            return this.finish(Err(Status::deadline_exceeded(
                                "etcdserver: request timed out; outcome unknown",
                            )));
                        }
                    };
                    let result = match reply {
                        Err(_) => Err(Status::unavailable("etcdserver: node unavailable")),
                        Ok(Ok(outcome)) => match outcome.result.map_err(|e| e.status()) {
                            Ok(LeaseReply::KeepAlive(response)) => Ok(response),
                            Ok(_) => Err(Status::internal("unexpected lease response")),
                            Err(error) => Err(error),
                        },
                        Ok(Err(error)) if error.error == "not leader" && !this.forwarded => {
                            this.forward(id, error.leader_id);
                            continue;
                        }
                        Ok(Err(error)) => {
                            Err(Status::unavailable(format!("etcdserver: {}", error.error)))
                        }
                    };
                    return this.finish(result);
                }
                KeepAliveState::Forward(future) => {
                    let result = ready!(future.as_mut().poll(cx));
                    return this.finish(result);
                }
            }
        }
    }
}
