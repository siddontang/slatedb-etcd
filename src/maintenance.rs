use crate::proto::etcdserverpb::*;
use serde::{Deserialize, Serialize};

pub const ALARMS: &[u8] = b"etcd/alarms";
pub const HASH: &[u8] = b"etcd/hash/";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Command {
    Status,
    HashAll,
    Hash(i64),
    Defragment,
    Snapshot,
    Alarm(AlarmRequest),
    MoveLeader(u64),
    Downgrade(DowngradeRequest),
}

pub fn has_put(request: &RequestOp) -> bool {
    use request_op::Request as Op;
    match &request.request {
        Some(Op::RequestPut(_)) => true,
        Some(Op::RequestTxn(r)) => r.success.iter().chain(&r.failure).any(has_put),
        _ => false,
    }
}
