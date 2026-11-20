//! Resolve current peer routes and fence local reads after forwarded writes.

use super::*;
use crate::node::ForwardView;
use std::sync::Mutex;
use tokio::sync::OnceCell;
use tonic::transport::Channel;

pub(super) struct Forwarding {
    view: OnceCell<ForwardView>,
    channels: Mutex<BTreeMap<u64, (String, Channel)>>,
    transport: crate::tls::Transport,
}

impl Forwarding {
    pub(super) fn new(
        peers: &BTreeMap<u64, String>,
        transport: crate::tls::Transport,
    ) -> anyhow::Result<Self> {
        let channels = peers
            .iter()
            .map(|(&id, url)| Ok((id, (crate::cluster::url(url), transport.channel(url)?))))
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            view: OnceCell::new(),
            channels: Mutex::new(channels),
            transport,
        })
    }
}

impl Service {
    pub(super) fn cached_remote_leader(&self) -> Option<u64> {
        let leader = self.forwarding.view.get()?.leader();
        (leader != 0 && leader != self.member).then_some(leader)
    }

    pub(super) async fn forward_kv(
        &self,
        leader: u64,
        op: Op,
        credentials: &crate::auth::Credentials,
    ) -> Result<ResponseOp, Status> {
        let channel = self.peer_channel(leader).await?;
        let mut client = KvClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
        let response = match op {
            Op::RequestRange(r) => Resp::ResponseRange(
                client
                    .range(forward_auth(r, credentials))
                    .await?
                    .into_inner(),
            ),
            Op::RequestPut(r) => {
                Resp::ResponsePut(client.put(forward_auth(r, credentials)).await?.into_inner())
            }
            Op::RequestDeleteRange(r) => Resp::ResponseDeleteRange(
                client
                    .delete_range(forward_auth(r, credentials))
                    .await?
                    .into_inner(),
            ),
            Op::RequestTxn(r) => {
                Resp::ResponseTxn(client.txn(forward_auth(r, credentials)).await?.into_inner())
            }
        };
        let mut response = ResponseOp {
            response: Some(response),
        };
        let revision = mvcc::response_header(&mut response)
            .ok_or_else(|| Status::internal("forwarded KV response has no header"))?
            .revision;
        // The leader already confirmed this result. Only local MVCC progress is missing.
        self.wait_revision(revision).await?;
        if let Some(header) = mvcc::response_header(&mut response) {
            header.member_id = self.member;
        }
        Ok(response)
    }

    async fn forward_view(&self) -> Result<&ForwardView, Status> {
        let unavailable = || Status::unavailable("etcdserver: node unavailable");
        if self.events.is_closed() {
            return Err(unavailable());
        }
        self.forwarding
            .view
            .get_or_try_init(|| async {
                let (tx, rx) = oneshot::channel();
                self.events
                    .send(Event::SubscribeForwarding(tx))
                    .await
                    .map_err(|_| unavailable())?;
                rx.await.map_err(|_| unavailable())
            })
            .await
    }

    pub(crate) async fn peer_channel(&self, id: u64) -> Result<Channel, Status> {
        let view = self.forward_view().await?;
        let routes = view.peers.borrow().clone();
        let address = routes
            .get(&id)
            .ok_or_else(|| Status::unavailable("etcdserver: no leader"))?;
        let cache = &self.forwarding.channels;
        #[cfg(feature = "write-profile")]
        let guard = crate::write_profile::Lock::Forwarding.acquire(cache);
        #[cfg(not(feature = "write-profile"))]
        let guard = cache.lock();
        let mut channels = guard.map_err(|_| Status::internal("peer channel lock failed"))?;
        if let Some((old, channel)) = channels.get(&id) {
            if old == address {
                return Ok(channel.clone());
            }
        }
        let channel = self
            .forwarding
            .transport
            .channel(address)
            .map_err(|e| Status::internal(e.to_string()))?;
        channels.retain(|peer, _| routes.contains_key(peer));
        channels.insert(id, (address.clone(), channel.clone()));
        Ok(channel)
    }

    pub(super) async fn wait_compaction(&self, revision: i64) -> Result<(), Status> {
        let mut durable = self.forward_view().await?.compact.clone();
        tokio::time::timeout(
            Duration::from_secs(10),
            durable.wait_for(|r| *r >= revision),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("local compaction flush timed out"))?
        .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        Ok(())
    }

    pub(crate) async fn wait_revision(&self, revision: i64) -> Result<(), Status> {
        let view = self.forward_view().await?;
        if view.applied(revision) {
            return Ok(());
        }
        let _permit = self.revision_admission.acquire().await?;
        if view.applied(revision) {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::WaitRevision(revision, tx))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: local apply timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?
            .map_err(|e| Status::unavailable(e.error))?;
        Ok(())
    }
}
