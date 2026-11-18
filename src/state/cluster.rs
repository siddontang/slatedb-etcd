use super::*;

impl StateMachine {
    pub async fn apply_cluster(
        &mut self,
        index: u64,
        request: &crate::cluster::Command,
        credentials: &crate::auth::Credentials,
    ) -> Result<Outcome<serde_json::Value>> {
        ensure!(index == self.applied + 1, "non-contiguous cluster apply");
        let allowed = if matches!(
            request,
            crate::cluster::Command::List(_) | crate::cluster::Command::Publish { .. }
        ) {
            Ok(())
        } else {
            self.auth.admin(credentials)
        };
        let mut members = self.members.clone();
        let result = allowed.and_then(|()| members.validate(request)).map(|()| {
            members.apply(request);
            let mut response = serde_json::json!({"header": mvcc::header(self.revision),
                "members": members.members.values().collect::<Vec<_>>()});
            if let crate::cluster::Command::Add(member) = request {
                response["member"] = serde_json::json!(member);
            }
            response
        });
        let mut batch = WriteBatch::new();
        batch.put(crate::cluster::KEY, serde_json::to_vec(&members)?);
        batch.put(APPLIED, index.to_be_bytes());
        self.write_batch(batch, index).await?;
        self.members = members;
        self.applied = index;
        Ok(Outcome { result })
    }
}
