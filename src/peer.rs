use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use protobuf::Message as ProtobufMessage;
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
) {
    // Drop the old backlog before reporting failure. Raft will retry on the new connection.
    for _ in 0..256 {
        let Ok(message) = messages.try_recv() else {
            break;
        };
        snapshot |= message.get_msg_type() == MessageType::MsgSnapshot;
    }
    report(events, peer, false, false).await;
    if snapshot {
        report(events, peer, true, false).await;
    }
}

pub(crate) async fn transport(
    peer: u64,
    address: String,
    cluster_id: String,
    mut messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    transport: crate::tls::Transport,
) {
    let Ok(channel) = transport.channel(&address) else {
        report(&events, peer, false, false).await;
        return;
    };
    let url = format!("{}/raft", crate::cluster::url(&address));
    while let Some(first) = messages.recv().await {
        if first.get_msg_type() == MessageType::MsgSnapshot {
            let success = post_snapshot(&transport.http, &url, &cluster_id, first).await;
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
            failed(&events, peer, &mut messages, false).await;
            continue;
        };
        let mut receipts = response.into_inner();
        let mut sent = 0;
        let mut acknowledged = 0_u64;
        let mut deadline = Instant::now() + PROGRESS_TIMEOUT;
        let mut snapshot = None;
        let mut healthy = false;
        let mut closed = false;
        let mut sizes = VecDeque::new();
        let mut outstanding_bytes = 0;
        let result: Result<()> = async {
            let send = |message: Message, sequence: u64| -> Result<usize> {
                let frame = Frame { sequence, payload: message.write_to_bytes()? };
                ensure!(prost::Message::encoded_len(&frame) <= FRAME_LIMIT, "peer frame is too large");
                let bytes = frame.payload.len();
                frames.try_send(frame)
                    .map_err(|_| anyhow::anyhow!("peer stream queue is unavailable"))?;
                Ok(bytes)
            };
            let bytes = send(first, 1)?;
            sizes.push_back(bytes);
            outstanding_bytes += bytes;
            sent += 1;
            loop {
                if acknowledged == sent {
                    if let Some(message) = snapshot.take() {
                        let success = post_snapshot(&transport.http, &url, &cluster_id, message).await;
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
                        outstanding_bytes -= sizes.pop_front().context("unexpected peer receipt")?;
                        deadline = Instant::now() + PROGRESS_TIMEOUT;
                        if !healthy { report(&events, peer, false, true).await; healthy = true; }
                    }
                    message = messages.recv(), if sizes.len() < WINDOW && outstanding_bytes < BYTE_TARGET && snapshot.is_none() => {
                        let Some(message) = message else { closed = true; return Ok(()); };
                        if message.get_msg_type() == MessageType::MsgSnapshot {
                            snapshot = Some(message);
                        } else {
                            if sent == acknowledged { deadline = Instant::now() + PROGRESS_TIMEOUT; }
                            sent = sent.checked_add(1).context("peer sequence exhausted")?;
                            let bytes = send(message, sent)?;
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
            failed(&events, peer, &mut messages, snapshot.is_some()).await;
        }
    }
}

async fn post_snapshot(
    client: &reqwest::Client,
    url: &str,
    cluster: &str,
    message: Message,
) -> bool {
    let result: Result<()> = async {
        let response = client
            .post(url)
            .header("x-cluster-id", cluster)
            .timeout(Duration::from_secs(30))
            .body(message.write_to_bytes()?)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "peer returned {}",
            response.status()
        );
        response.bytes().await.context("read snapshot response")?;
        Ok(())
    }
    .await;
    result.is_ok()
}

#[cfg(test)]
mod tests;
