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
        if request
            .metadata()
            .get("x-cluster-id")
            .and_then(|v| v.to_str().ok())
            != Some(self.0.cluster_id.as_str())
        {
            return Err(Status::permission_denied("peer cluster does not match"));
        }
        let incoming = request.into_inner();
        let events = self.0.events.clone();
        let (responses, stream) = mpsc::channel(WINDOW);
        tokio::spawn(async move {
            if let Err(error) = receive::run(incoming, events, &responses).await {
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

/// Send to a fixed address. Tests use this form.
#[cfg(test)]
pub(crate) async fn transport(
    peer: u64,
    address: String,
    cluster_id: String,
    messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    transport: crate::tls::Transport,
    traffic: std::sync::Arc<crate::metrics::PeerTraffic>,
) {
    let (_address, receiver) = tokio::sync::watch::channel(address);
    transport_to(
        peer, receiver, cluster_id, messages, events, transport, traffic,
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
    cluster_id: String,
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
            let success = post_snapshot(&transport.http, &url, &cluster_id, first, &traffic).await;
            report(&events, peer, true, success).await;
            continue;
        }
        let mut client =
            TransportClient::new(channel.clone()).max_encoding_message_size(FRAME_LIMIT);
        let (frames, stream) = mpsc::channel(WINDOW);
        let mut request = Request::new(ReceiverStream::new(stream));
        let Ok(cluster) = cluster_id.parse() else {
            report(&events, peer, false, false).await;
            return;
        };
        request.metadata_mut().insert("x-cluster-id", cluster);
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
                        let success = post_snapshot(&transport.http, &url, &cluster_id, message, &traffic).await;
                        report(&events, peer, true, success).await;
                        healthy = success;
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

async fn post_snapshot(
    client: &reqwest::Client,
    url: &str,
    cluster: &str,
    message: Message,
    traffic: &crate::metrics::PeerTraffic,
) -> bool {
    let result: Result<()> = async {
        let payload = message.encode_to_vec();
        let size = payload.len();
        let response = client
            .post(url)
            .header("x-cluster-id", cluster)
            .timeout(Duration::from_secs(30))
            .body(payload)
            .send()
            .await?;
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
    if result.is_err() {
        traffic.failed(1);
    }
    result.is_ok()
}

#[cfg(test)]
mod tests;
