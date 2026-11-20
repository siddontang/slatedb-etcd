use super::*;
use std::sync::Arc;

pub(super) const MAX_TASKS: usize = 4;

struct Work {
    view: Arc<crate::state::ReadView>,
    queries: Vec<Query>,
    header: Header,
    wide: bool,
}

pub(in crate::node) struct Executor {
    tasks: tokio::task::JoinSet<(bool, Result<Output>)>,
    wide_jobs: usize,
    #[cfg(test)]
    manual: Option<VecDeque<Work>>,
}

impl Executor {
    pub fn new(manual: bool) -> Self {
        #[cfg(not(test))]
        assert!(!manual, "manual reads are only available in tests");
        Self {
            tasks: tokio::task::JoinSet::new(),
            wide_jobs: 0,
            #[cfg(test)]
            manual: manual.then(VecDeque::new),
        }
    }

    pub fn len(&self) -> usize {
        #[cfg(test)]
        if let Some(jobs) = &self.manual {
            return jobs.len();
        }
        self.tasks.len()
    }

    pub fn has_capacity(&self) -> bool {
        self.len() < MAX_TASKS
    }

    pub fn can_submit(&self, query: &Query) -> bool {
        self.has_capacity() && (query.is_point_read() || self.wide_jobs == 0)
    }

    pub fn submit(
        &mut self,
        view: Arc<crate::state::ReadView>,
        queries: Vec<Query>,
        header: Header,
    ) {
        assert!(self.has_capacity());
        assert!(!queries.is_empty() && queries.len() <= apply::READ_BATCH);
        let wide = !queries.iter().all(Query::is_point_read);
        assert!(!wide || self.wide_jobs == 0);
        self.wide_jobs += usize::from(wide);
        let work = Work {
            view,
            queries,
            header,
            wide,
        };
        #[cfg(test)]
        if let Some(jobs) = &mut self.manual {
            jobs.push_back(work);
            return;
        }
        self.tasks
            .spawn(async move { (work.wide, execute(work).await) });
    }

    pub async fn next(&mut self) -> Result<Output> {
        let (wide, output) = self
            .tasks
            .join_next()
            .await
            .context("read worker stopped")??;
        self.wide_jobs -= usize::from(wide);
        output
    }

    pub async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
    }

    #[cfg(test)]
    pub async fn step(&mut self) -> Result<Option<Output>> {
        let jobs = self.manual.as_mut().context("read worker is not manual")?;
        match jobs.pop_front() {
            Some(work) => {
                self.wide_jobs -= usize::from(work.wide);
                execute(work).await.map(Some)
            }
            None => Ok(None),
        }
    }
}

async fn execute(work: Work) -> Result<Output> {
    let mut outputs = Vec::with_capacity(work.queries.len());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut queries = work.queries.into_iter();
    while let Some(query) = queries.next() {
        let (request, credentials) = match &query {
            Query::Read(request, credentials, _) | Query::Linear(request, credentials, _) => {
                (request, credentials)
            }
            _ => anyhow::bail!("read worker received a state operation"),
        };
        let mut outcome =
            match tokio::time::timeout_at(deadline, work.view.read(request, credentials.as_ref()))
                .await
            {
                Ok(outcome) => outcome?,
                Err(_) => crate::mvcc::Outcome {
                    result: Err(crate::mvcc::KvError::new(
                        tonic::Code::DeadlineExceeded,
                        "read execution timed out",
                    )),
                },
            };
        work.header.outcome(&mut outcome);
        if work.wide && queries.len() > 0 {
            // Each request already has a quorum proof. All read the same pinned view.
            let ids = std::iter::once(query)
                .chain(queries)
                .map(|query| match query {
                    Query::Linear(_, _, id) => id,
                    _ => unreachable!(),
                })
                .collect();
            return Ok(Output::SharedLinear(ids, Arc::new(outcome)));
        }
        outputs.push(match query {
            Query::Read(_, _, response) => Output::Read { response, outcome },
            Query::Linear(_, _, id) => Output::Linear { id, outcome },
            _ => unreachable!(),
        });
    }
    Ok(Output::Reads(outputs))
}
