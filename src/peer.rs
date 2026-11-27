use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use prost::Message as ProtobufMessage;
use raft::eraftpb::{Message, MessageType};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::api::ApiState;
use crate::node::Event;
use crate::proto::peer::{
    transport_client::TransportClient,
    transport_server::{Transport, TransportServer},
    Frame, Receipt,
};

mod receive;

pub(crate) fn decode_message(payload: bytes::Bytes) -> Result<Message> {
    let message = Message::decode(payload)?;
    MessageType::try_from(message.msg_type)?;
    for entry in &message.entries {
        raft::eraftpb::EntryType::try_from(entry.entry_type)?;
    }
    Ok(message)
}

const WINDOW: usize = 64;
const BYTE_TARGET: usize = 16 * 1024 * 1024;
const FRAME_LIMIT: usize = 8 * 1024 * 1024 + 1024;
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct Service(ApiState);

pub(crate) fn service(state: ApiState) -> TransportServer<Service> {
    TransportServer::new(Service(state)).max_decoding_message_size(FRAME_LIMIT)
}

#[tonic::async_trait]
impl Transport for Service {
    type ExchangeStream = ReceiverStream<Result<Receipt, Status>>;

    async fn exchange(
        &self,
        request: Request<tonic::Streaming<Frame>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let header = |name| request.metadata().get(name).and_then(|v| v.to_str().ok());
        self.0
            .admit(header(CLUSTER), header(STORE))
            .map_err(Status::permission_denied)?;
        let process = process_of(
            request
                .metadata()
                .get(PROCESS)
                .map(|value| value.as_bytes()),
        );
        let incoming = request.into_inner();
        let events = self.0.events.clone();
        let (responses, stream) = mpsc::channel(WINDOW);
        tokio::spawn(async move {
            if let Err(error) = receive::run(incoming, events, &responses, process).await {
                let _ = responses.send(Err(error)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(stream)))
    }
}

async fn report(events: &mpsc::Sender<Event>, peer: u64, snapshot: bool, success: bool) {
    let _ = events
        .send(Event::NetworkResult {
            peer,
            snapshot,
            success,
        })
        .await;
}

async fn failed(
    events: &mpsc::Sender<Event>,
    peer: u64,
    messages: &mut mpsc::Receiver<Message>,
    mut snapshot: bool,
    mut undelivered: usize,
    traffic: &crate::metrics::PeerTraffic,
) {
    // Drop the old backlog before reporting failure. Raft will retry on the new connection.
    for _ in 0..256 {
        let Ok(message) = messages.try_recv() else {
            break;
        };
        snapshot |= message.get_msg_type() == MessageType::MsgSnapshot;
        undelivered += 1;
    }
    traffic.failed(undelivered);
    report(events, peer, false, false).await;
    if snapshot {
        report(events, peer, true, false).await;
    }
}

/// The header and metadata name of the sender's cluster ID on peer traffic.
pub(crate) const CLUSTER: &str = "x-cluster-id";
/// The header and metadata name of the sender's shared store ID on peer traffic.
pub(crate) const STORE: &str = "x-store-id";

/// The cluster and the shared store that a member names on its peer traffic.
#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub cluster: String,
    /// The store ID of the cluster claim.
    pub store: Option<String>,
}

/// The part of a peer identity that differs from the identity of this member.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Mismatch {
    Cluster,
    Store,
}

#[cfg(test)]
impl From<&str> for Identity {
    fn from(cluster: &str) -> Self {
        Self {
            cluster: cluster.to_owned(),
            store: None,
        }
    }
}

impl Identity {
    /// Check the cluster and the store that a peer names. A member admits exactly the identity
    /// that it sends.
    ///
    /// A member on another bucket or prefix cannot open the checkpoints of this store. So its
    /// traffic stops before Raft can count it in a quorum. A peer that names no store is also
    /// refused.
    pub(crate) fn admits(
        &self,
        cluster: Option<&str>,
        store: Option<&str>,
    ) -> Result<(), Mismatch> {
        if cluster != Some(self.cluster.as_str()) {
            return Err(Mismatch::Cluster);
        }
        if store != self.store.as_deref() {
            return Err(Mismatch::Store);
        }
        Ok(())
    }

    /// Add the identity headers to a peer HTTP request.
    pub(crate) fn headers(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let request = request.header(CLUSTER, &self.cluster);
        match &self.store {
            Some(store) => request.header(STORE, store),
            None => request,
        }
    }
}

/// The header and metadata name of the sender's process ID on peer traffic.
pub(crate) const PROCESS: &str = "x-member-process";
/// The route of a view snapshot above its checkpoint row.
pub(crate) const COVERED_ROUTE: &str = "/raft/covered";
/// The header that names the receiver process of a snapshot on `COVERED_ROUTE`.
pub(crate) const TARGET_PROCESS: &str = "x-target-process";

/// A random ID of this OS process. A member sends it with its peer traffic and its `ViewAck`
/// reports, so the leader knows when a member runs a new process.
pub(crate) fn process_id() -> u64 {
    static ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *ID.get_or_init(|| uuid::Uuid::new_v4().as_u64_pair().0)
}

/// Read a process ID from peer request headers, if they hold one.
pub(crate) fn process_of(value: Option<&[u8]>) -> Option<u64> {
    std::str::from_utf8(value?).ok()?.parse().ok()
}

/// Send to a fixed address. Tests use this form.
#[cfg(test)]
pub(crate) async fn transport(
    peer: u64,
    address: String,
    identity: Identity,
    messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    transport: crate::tls::Transport,
    traffic: std::sync::Arc<crate::metrics::PeerTraffic>,
) {
    let (_address, receiver) = tokio::sync::watch::channel(address);
    transport_to(
        peer, receiver, identity, messages, events, transport, traffic,
    )
    .await;
}

/// Send Raft messages to one peer.
///
/// Like etcd, a changed peer URL applies when the sender dials again. A healthy stream keeps its
/// connection, so a member update to an unreachable URL does not cut off a working peer.
pub(crate) async fn transport_to(
    peer: u64,
    mut address: tokio::sync::watch::Receiver<String>,
    identity: Identity,
    mut messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    transport: crate::tls::Transport,
    traffic: std::sync::Arc<crate::metrics::PeerTraffic>,
) {
    let current = address.borrow_and_update().clone();
    let Ok(mut channel) = transport.channel(&current) else {
        report(&events, peer, false, false).await;
        return;
    };
    let mut url = format!("{}/raft", crate::cluster::url(&current));
    while let Some(first) = messages.recv().await {
        if address.has_changed().unwrap_or(false) {
            let next = address.borrow_and_update().clone();
            if let Ok(next_channel) = transport.channel(&next) {
                channel = next_channel;
                url = format!("{}/raft", crate::cluster::url(&next));
            }
        }
        if first.get_msg_type() == MessageType::MsgSnapshot {
            spawn_snapshot(
                &transport.http,
                &url,
                &identity,
                first,
                &traffic,
                &events,
                peer,
            );
            continue;
        }
        let mut client =
            TransportClient::new(channel.clone()).max_encoding_message_size(FRAME_LIMIT);
        let (frames, stream) = mpsc::channel(WINDOW);
        let mut request = Request::new(ReceiverStream::new(stream));
        let Ok(cluster) = identity.cluster.parse() else {
            report(&events, peer, false, false).await;
            return;
        };
        request.metadata_mut().insert(CLUSTER, cluster);
        if let Some(store) = &identity.store {
            let Ok(store) = store.parse() else {
                report(&events, peer, false, false).await;
                return;
            };
            request.metadata_mut().insert(STORE, store);
        }
        if let Ok(process) = process_id().to_string().parse() {
            request.metadata_mut().insert(PROCESS, process);
        }
        let result = tokio::time::timeout(PROGRESS_TIMEOUT, client.exchange(request)).await;
        let Ok(Ok(response)) = result else {
            failed(&events, peer, &mut messages, false, 1, &traffic).await;
            continue;
        };
        let mut receipts = response.into_inner();
        let mut sent = 0;
        let mut acknowledged = 0_u64;
        let start = Instant::now();
        let mut deadline = start + PROGRESS_TIMEOUT;
        let mut probe = Some((1, start));
        let mut next_probe = start + Duration::from_secs(5);
        let mut probe_due = false;
        let mut snapshot = None;
        let mut healthy = false;
        let mut closed = false;
        let mut sizes = VecDeque::new();
        let mut outstanding_bytes = 0;
        let result: Result<()> = async {
            let send = |message: Message, sequence: u64| -> Result<usize> {
                let frame = Frame { sequence, payload: message.encode_to_vec() };
                ensure!(prost::Message::encoded_len(&frame) <= FRAME_LIMIT, "peer frame is too large");
                let bytes = frame.payload.len();
                frames.try_send(frame)
                    .map_err(|_| anyhow::anyhow!("peer stream queue is unavailable"))?;
                Ok(bytes)
            };
            let bytes = send(first, 1).inspect_err(|_| traffic.failed(1))?;
            sizes.push_back(bytes);
            outstanding_bytes += bytes;
            sent += 1;
            loop {
                if acknowledged == sent {
                    if let Some(message) = snapshot.take() {
                        spawn_snapshot(&transport.http, &url, &identity, message, &traffic, &events, peer);
                    }
                }
                tokio::select! {
                    receipt = receipts.message() => {
                        let receipt = receipt?.context("peer stream closed")?;
                        ensure!(Some(receipt.sequence) == acknowledged.checked_add(1) && receipt.sequence <= sent,
                            "peer receipt is out of order");
                        acknowledged = receipt.sequence;
                        let bytes = sizes.pop_front().context("unexpected peer receipt")?;
                        outstanding_bytes -= bytes;
                        traffic.sent.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
                        let now = Instant::now();
                        deadline = now + PROGRESS_TIMEOUT;
                        if let Some((sequence, started)) = probe {
                            if acknowledged == sequence {
                                traffic.observe_rtt(peer, now - started);
                                probe = None;
                            }
                        }
                        probe_due |= now >= next_probe;
                        if !healthy { report(&events, peer, false, true).await; healthy = true; }
                    }
                    message = messages.recv(), if sizes.len() < WINDOW && outstanding_bytes < BYTE_TARGET && snapshot.is_none() => {
                        let Some(message) = message else { closed = true; return Ok(()); };
                        if message.get_msg_type() == MessageType::MsgSnapshot {
                            snapshot = Some(message);
                        } else {
                            if sent == acknowledged { deadline = Instant::now() + PROGRESS_TIMEOUT; }
                            sent = sent.checked_add(1).context("peer sequence exhausted")?;
                            if probe_due && probe.is_none() {
                                let now = Instant::now();
                                probe = Some((sent, now));
                                next_probe = now + Duration::from_secs(5);
                                probe_due = false;
                            }
                            let bytes = send(message, sent).inspect_err(|_| traffic.failed(1))?;
                            sizes.push_back(bytes);
                            outstanding_bytes += bytes;
                        }
                    }
                    _ = tokio::time::sleep_until(deadline), if acknowledged < sent => {
                        anyhow::bail!("peer stream made no progress");
                    }
                }
            }
        }.await;
        if closed {
            return;
        }
        if result.is_err() {
            let lost = sizes.len() + usize::from(snapshot.is_some());
            failed(
                &events,
                peer,
                &mut messages,
                snapshot.is_some(),
                lost,
                &traffic,
            )
            .await;
        }
    }
}

/// Send a snapshot in its own task. The follower copies the checkpoint objects before it
/// replies, so the request can take minutes. Heartbeats continue on the stream meanwhile.
fn spawn_snapshot(
    client: &reqwest::Client,
    url: &str,
    identity: &Identity,
    message: Message,
    traffic: &std::sync::Arc<crate::metrics::PeerTraffic>,
    events: &mpsc::Sender<Event>,
    peer: u64,
) {
    let (client, url, identity) = (client.clone(), url.to_owned(), identity.clone());
    let (traffic, events) = (traffic.clone(), events.clone());
    tokio::spawn(async move {
        let target = <[u8; 8]>::try_from(message.context.as_ref()).map(u64::from_le_bytes);
        let delivery = post_snapshot(&client, &url, &identity, message, &traffic).await;
        // The member does not run the process that the snapshot names. That process loses the
        // capability, but a later process that reported it keeps it.
        if let (Delivery::Refused, Ok(target)) = (delivery, target) {
            let _ = events
                .send(Event::SnapshotRefused {
                    member: peer,
                    target,
                })
                .await;
        }
        report(&events, peer, true, delivery == Delivery::Sent).await;
    });
}

/// The result of one snapshot request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delivery {
    Sent,
    Failed,
    /// The receiver has no route for a snapshot above its row, or it runs another process.
    Refused,
}

/// Send one snapshot. The node names a receiver process in the context of a view snapshot above
/// its checkpoint row, and zero when it knows none. Such a snapshot goes to `COVERED_ROUTE`.
/// Another process answers 409 before Raft sees the snapshot.
async fn post_snapshot(
    client: &reqwest::Client,
    url: &str,
    identity: &Identity,
    message: Message,
    traffic: &crate::metrics::PeerTraffic,
) -> Delivery {
    let target = <[u8; 8]>::try_from(message.context.as_ref())
        .ok()
        .map(u64::from_le_bytes);
    let covered = target.is_some();
    let mut refused = false;
    let result: Result<()> = async {
        let payload = message.encode_to_vec();
        let size = payload.len();
        let mut request = match (covered, url.strip_suffix("/raft")) {
            (true, Some(base)) => client.post(format!("{base}{COVERED_ROUTE}")),
            _ => client.post(url),
        };
        if let (true, Some(target)) = (covered, target) {
            request = request.header(TARGET_PROCESS, target.to_string());
        }
        let response = identity
            .headers(request)
            .header(PROCESS, process_id().to_string())
            // The follower copies the checkpoint objects before it replies. A retry continues
            // from the objects that it already copied.
            .timeout(Duration::from_secs(600))
            .body(payload)
            .send()
            .await?;
        refused = covered
            && matches!(
                response.status(),
                reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::CONFLICT
            );
        ensure!(
            response.status().is_success(),
            "peer returned {}",
            response.status()
        );
        response.bytes().await.context("read snapshot response")?;
        traffic
            .sent
            .fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Delivery::Sent,
        Err(_) => {
            traffic.failed(1);
            match refused {
                true => Delivery::Refused,
                false => Delivery::Failed,
            }
        }
    }
}

#[cfg(test)]
mod tests;
