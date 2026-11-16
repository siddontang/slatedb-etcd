use std::collections::BTreeMap;
use std::time::Duration;

use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

mod lease;
mod watch;

use crate::mvcc::{self, Outcome};
use crate::node::Event;
use crate::proto::etcdserverpb::kv_client::KvClient;
use crate::proto::etcdserverpb::kv_server::{Kv, KvServer};
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;

#[derive(Clone)]
pub struct Service {
    events: mpsc::Sender<Event>,
    peers: BTreeMap<u64, Channel>,
    member: u64,
}

pub fn service(
    member: u64,
    events: mpsc::Sender<Event>,
    peers: &BTreeMap<u64, String>,
) -> axum::Router {
    let peers = peers
        .iter()
        .map(|(&id, address)| {
            let endpoint = Endpoint::from_shared(format!("http://{address}"))
                .expect("validated peer address")
                .timeout(Duration::from_secs(10));
            (id, endpoint.connect_lazy())
        })
        .collect();
    let service = Service {
        events,
        peers,
        member,
    };
    axum::Router::new()
        .route_service(
            "/etcdserverpb.KV/{*method}",
            KvServer::new(service.clone())
                .max_decoding_message_size(1536 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .route_service(
            "/etcdserverpb.Lease/{*method}",
            lease_server::LeaseServer::new(service.clone()),
        )
        .route_service(
            "/etcdserverpb.Watch/{*method}",
            watch_server::WatchServer::new(service).max_encoding_message_size(64 * 1024 * 1024),
        )
}

impl Service {
    async fn execute(&self, op: Op, forwarded: bool) -> Result<ResponseOp, Status> {
        let request = RequestOp {
            request: Some(op.clone()),
        };
        if request.encoded_len() > 1536 * 1024 {
            return Err(Status::resource_exhausted(
                "etcdserver: request is too large",
            ));
        }
        mvcc::validate(&request).map_err(|e| e.status())?;
        let (tx, rx) = oneshot::channel();
        let local = match &op {
            Op::RequestRange(r) => r.serializable,
            Op::RequestTxn(t) => t
                .success
                .iter()
                .chain(&t.failure)
                .all(|op| matches!(&op.request, Some(Op::RequestRange(r)) if r.serializable)),
            _ => false,
        };
        let event = if local {
            Event::EtcdRead(request, tx)
        } else {
            Event::EtcdPropose(request, tx)
        };
        self.events
            .try_send(event)
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: request timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        match reply {
            Ok(value) => {
                let outcome: Outcome =
                    serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
                outcome.result.map_err(|e| e.status())
            }
            Err(error) if error.error == "not leader" && !forwarded => {
                let channel = self
                    .peers
                    .get(&error.leader_id)
                    .ok_or_else(|| Status::unavailable("etcdserver: no leader"))?;
                let mut client =
                    KvClient::new(channel.clone()).max_decoding_message_size(64 * 1024 * 1024);
                let response = match op {
                    Op::RequestRange(r) => {
                        Resp::ResponseRange(client.range(forward(r)).await?.into_inner())
                    }
                    Op::RequestPut(r) => {
                        Resp::ResponsePut(client.put(forward(r)).await?.into_inner())
                    }
                    Op::RequestDeleteRange(r) => Resp::ResponseDeleteRange(
                        client.delete_range(forward(r)).await?.into_inner(),
                    ),
                    Op::RequestTxn(r) => {
                        Resp::ResponseTxn(client.txn(forward(r)).await?.into_inner())
                    }
                };
                let mut response = ResponseOp {
                    response: Some(response),
                };
                if let Some(header) = mvcc::response_header(&mut response) {
                    header.member_id = self.member;
                }
                Ok(response)
            }
            Err(error) => Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        }
    }
}

fn forward<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("x-slatedb-forwarded", "true".parse().unwrap());
    request
}

fn is_forwarded<T>(request: &Request<T>) -> bool {
    request.metadata().contains_key("x-slatedb-forwarded")
}

#[tonic::async_trait]
impl Kv for Service {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestRange(request.into_inner()), forwarded)
            .await?
            .response
        {
            Some(Resp::ResponseRange(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestPut(request.into_inner()), forwarded)
            .await?
            .response
        {
            Some(Resp::ResponsePut(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestDeleteRange(request.into_inner()), forwarded)
            .await?
            .response
        {
            Some(Resp::ResponseDeleteRange(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestTxn(request.into_inner()), forwarded)
            .await?
            .response
        {
            Some(Resp::ResponseTxn(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let forwarded = is_forwarded(&request);
        let request = request.into_inner();
        if request.physical {
            return Err(Status::unimplemented(
                "etcdserver: physical MVCC compaction is not implemented",
            ));
        }
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::EtcdCompact(request, tx))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: request timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut response = match reply {
            Ok(value) => {
                let outcome: Outcome<CompactionResponse> =
                    serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
                outcome.result.map_err(|e| e.status())?
            }
            Err(error) if error.error == "not leader" && !forwarded => {
                let channel = self
                    .peers
                    .get(&error.leader_id)
                    .ok_or_else(|| Status::unavailable("etcdserver: no leader"))?;
                KvClient::new(channel.clone())
                    .compact(forward(request))
                    .await?
                    .into_inner()
            }
            Err(error) => return Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        };
        if let Some(header) = &mut response.header {
            header.member_id = self.member;
        }
        Ok(Response::new(response))
    }
}
