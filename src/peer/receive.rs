use super::*;
use tokio::sync::oneshot;

struct Pending {
    sequence: u64,
    bytes: usize,
    deadline: Instant,
    completed: oneshot::Receiver<Result<serde_json::Value, crate::ApiError>>,
}

pub(super) async fn run(
    mut incoming: tonic::Streaming<Frame>,
    events: mpsc::Sender<Event>,
    responses: &mpsc::Sender<Result<Receipt, Status>>,
) -> Result<(), Status> {
    let mut sequence = 0_u64;
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut bytes = 0;
    let mut closed = false;
    loop {
        if closed && pending.is_empty() {
            return Ok(());
        }
        let deadline = pending
            .front()
            .map_or_else(Instant::now, |first| first.deadline);
        tokio::select! {
            _ = responses.closed() => return Ok(()),
            frame = incoming.message(), if !closed && pending.len() < WINDOW && bytes < BYTE_TARGET => {
                let Some(frame) = frame? else {
                    closed = true;
                    continue;
                };
                if Some(frame.sequence) != sequence.checked_add(1) {
                    return Err(Status::invalid_argument("peer sequence is out of order"));
                }
                sequence = frame.sequence;
                let size = frame.payload.len();
                let message = decode_message(frame.payload.into())
                    .map_err(|_| Status::invalid_argument("invalid Raft message"))?;
                if message.get_msg_type() == MessageType::MsgSnapshot {
                    return Err(Status::invalid_argument("snapshots require the durable endpoint"));
                }
                let (response, completed) = oneshot::channel();
                events.try_send(Event::PeerFrame(message, size, response))
                    .map_err(|_| Status::unavailable("peer queue is unavailable"))?;
                bytes += size;
                pending.push_back(Pending {
                    sequence,
                    bytes: size,
                    deadline: Instant::now() + Duration::from_secs(5),
                    completed,
                });
            }
            result = async { (&mut pending.front_mut().unwrap().completed).await }, if !pending.is_empty() => {
                result.map_err(|_| Status::unavailable("peer stopped"))?
                    .map_err(|error| Status::unavailable(error.error))?;
                let first = pending.pop_front().unwrap();
                bytes -= first.bytes;
                responses.send(Ok(Receipt { sequence: first.sequence })).await
                    .map_err(|_| Status::cancelled("peer stream closed"))?;
            }
            _ = tokio::time::sleep_until(deadline), if !pending.is_empty() => {
                return Err(Status::deadline_exceeded("peer receipt timed out"));
            }
        }
    }
}
