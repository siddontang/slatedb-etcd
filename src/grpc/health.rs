use super::*;
use tokio_stream::wrappers::ReceiverStream;
use tonic_health::pb::{
    health_check_response::ServingStatus, HealthCheckRequest, HealthCheckResponse,
};

#[tonic::async_trait]
impl tonic_health::pb::health_server::Health for Service {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        if !matches!(request.get_ref().service.as_str(), "" | "etcdserverpb.KV") {
            return Err(Status::not_found("unknown service"));
        }
        let status = if self.local_barrier().await.is_ok() {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        Ok(Response::new(HealthCheckResponse {
            status: status as i32,
        }))
    }
    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let service = self.clone();
        let body = request.into_inner();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut previous = None;
            loop {
                let result = service
                    .check(Request::new(body.clone()))
                    .await
                    .map(Response::into_inner);
                let status = result.as_ref().ok().map(|r| r.status);
                if previous != status || result.is_err() {
                    let failed = result.is_err();
                    if tx.send(result).await.is_err() || failed {
                        break;
                    }
                    previous = status;
                }
                tokio::select! { _ = tx.closed() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => () }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
