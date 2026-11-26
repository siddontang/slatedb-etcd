use super::*;
use std::sync::Arc;

pub(in crate::node) const MAX_TASKS: usize = 4;
/// Streaming Ranges that can run at once. One task stays available for point reads.
pub(in crate::node) const MAX_SCANS: usize = MAX_TASKS - 1;
/// Bytes of rows that one shared scan can materialize.
/// A larger result runs again in the exclusive slot on the same pinned view.
pub(in crate::node) const SCAN_ROW_BUDGET: usize = 4 << 20;
/// Execution time of a point read.
const POINT_READ_LIMIT: Duration = Duration::from_secs(5);

/// The executor capacity class of a read job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node) enum Slot {
    /// Current single-key Ranges.
    Point,
    /// Current key-ordered Ranges without sorting or revision filters, under a row budget.
    Scan,
    /// Other reads, and scans over their budget. They run without other wide reads.
    Exclusive,
}

impl Slot {
    pub fn of(query: &Query) -> Self {
        if query.is_point_read() {
            Self::Point
        } else if query.is_streaming_range() {
            Self::Scan
        } else {
            Self::Exclusive
        }
    }
}

struct Work {
    view: Arc<crate::state::ReadView>,
    queries: Vec<Query>,
    header: Header,
    slot: Slot,
    deadline: tokio::time::Instant,
    row_budget: usize,
}

enum Done {
    Output(Result<Output>),
    /// A scan exceeded its row budget. It keeps its pinned view, queries, and deadline.
    OverBudget(Work),
}

pub(in crate::node) struct Executor {
    tasks: tokio::task::JoinSet<(Slot, Done)>,
    scans: usize,
    exclusive: bool,
    /// Scans over budget, waiting for the exclusive slot. They block new wide reads.
    over_budget: VecDeque<Work>,
    pub(in crate::node) scan_row_budget: usize,
    /// Scans that ran again in the exclusive slot.
    #[cfg(test)]
    pub(in crate::node) reruns: usize,
    #[cfg(test)]
    manual: Option<VecDeque<Work>>,
}

impl Executor {
    pub fn new(manual: bool) -> Self {
        #[cfg(not(test))]
        assert!(!manual, "manual reads are only available in tests");
        Self {
            tasks: tokio::task::JoinSet::new(),
            scans: 0,
            exclusive: false,
            over_budget: VecDeque::new(),
            scan_row_budget: SCAN_ROW_BUDGET,
            #[cfg(test)]
            reruns: 0,
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
        self.has_capacity()
            && match Slot::of(query) {
                Slot::Point => true,
                Slot::Scan => self.wide_idle() && self.scans < MAX_SCANS,
                Slot::Exclusive => self.wide_idle() && self.scans == 0,
            }
    }

    /// No exclusive read runs or waits.
    fn wide_idle(&self) -> bool {
        !self.exclusive && self.over_budget.is_empty()
    }

    pub fn submit(
        &mut self,
        view: Arc<crate::state::ReadView>,
        queries: Vec<Query>,
        header: Header,
    ) {
        assert!(!queries.is_empty() && queries.len() <= apply::READ_BATCH);
        assert!(self.can_submit(&queries[0]));
        let slot = Slot::of(&queries[0]);
        assert!(queries.iter().all(|query| Slot::of(query) == slot));
        let work = Work {
            view,
            queries,
            header,
            slot,
            deadline: tokio::time::Instant::now()
                + match slot {
                    Slot::Point => POINT_READ_LIMIT,
                    Slot::Scan | Slot::Exclusive => crate::node::WIDE_READ_LIMIT,
                },
            row_budget: self.scan_row_budget,
        };
        self.start(work);
    }

    fn start(&mut self, work: Work) {
        match work.slot {
            Slot::Point => {}
            Slot::Scan => self.scans += 1,
            Slot::Exclusive => self.exclusive = true,
        }
        #[cfg(test)]
        if let Some(jobs) = &mut self.manual {
            jobs.push_back(work);
            return;
        }
        self.tasks
            .spawn(async move { (work.slot, execute(work).await) });
    }

    fn release(&mut self, slot: Slot) {
        match slot {
            Slot::Point => {}
            Slot::Scan => self.scans -= 1,
            Slot::Exclusive => self.exclusive = false,
        }
    }

    /// Start the oldest scan over budget once no other wide read runs.
    fn start_over_budget(&mut self) {
        if self.has_capacity() && !self.exclusive && self.scans == 0 {
            if let Some(mut work) = self.over_budget.pop_front() {
                work.slot = Slot::Exclusive;
                self.start(work);
            }
        }
    }

    fn finish(&mut self, slot: Slot, done: Done) -> Option<Result<Output>> {
        self.release(slot);
        let output = match done {
            Done::Output(output) => Some(output),
            Done::OverBudget(work) => {
                #[cfg(test)]
                {
                    self.reruns += 1;
                }
                self.over_budget.push_back(work);
                None
            }
        };
        self.start_over_budget();
        output
    }

    pub async fn next(&mut self) -> Result<Output> {
        loop {
            let (slot, done) = self
                .tasks
                .join_next()
                .await
                .context("read worker stopped")??;
            if let Some(output) = self.finish(slot, done) {
                return output;
            }
        }
    }

    pub async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
        self.over_budget.clear();
    }

    /// True if every queued manual job still has time left to run.
    #[cfg(test)]
    pub fn manual_jobs_have_time(&self) -> bool {
        let now = tokio::time::Instant::now();
        self.manual.iter().flatten().all(|work| work.deadline > now)
    }

    #[cfg(test)]
    pub async fn step(&mut self) -> Result<Option<Output>> {
        loop {
            let jobs = self.manual.as_mut().context("read worker is not manual")?;
            let Some(work) = jobs.pop_front() else {
                return Ok(None);
            };
            let slot = work.slot;
            if let Some(output) = self.finish(slot, execute(work).await) {
                return output.map(Some);
            }
        }
    }
}

fn request(query: &Query) -> Result<(&RequestOp, Option<&crate::auth::Credentials>)> {
    match query {
        Query::Read(request, credentials, _) | Query::Linear(request, credentials, _) => {
            Ok((request, credentials.as_ref()))
        }
        _ => anyhow::bail!("read worker received a state operation"),
    }
}

fn timed_out() -> crate::mvcc::Outcome {
    crate::mvcc::Outcome {
        result: Err(crate::mvcc::KvError::new(
            tonic::Code::DeadlineExceeded,
            "read execution timed out",
        )),
    }
}

async fn execute(work: Work) -> Done {
    match work.slot {
        Slot::Point => Done::Output(execute_points(work).await),
        Slot::Scan | Slot::Exclusive => execute_wide(work).await,
    }
}

/// Evaluate a wide job once. All queries are the same request with the same credentials.
async fn execute_wide(work: Work) -> Done {
    let budget = (work.slot == Slot::Scan).then_some(work.row_budget);
    let read = async {
        let (request, credentials) = request(&work.queries[0])?;
        work.view.read_within(request, credentials, budget).await
    };
    let mut outcome = match tokio::time::timeout_at(work.deadline, read).await {
        Ok(Ok(Some(outcome))) => outcome,
        Ok(Ok(None)) => return Done::OverBudget(work),
        Ok(Err(error)) => return Done::Output(Err(error)),
        Err(_) => timed_out(),
    };
    work.header.outcome(&mut outcome);
    let mut queries = work.queries;
    if queries.len() > 1 {
        // Each request already has a quorum proof. All read the same pinned view.
        let ids = queries
            .into_iter()
            .map(|query| match query {
                Query::Linear(_, _, id) => id,
                _ => unreachable!(),
            })
            .collect();
        return Done::Output(Ok(Output::SharedLinear(ids, Arc::new(outcome))));
    }
    Done::Output(Ok(Output::Reads(vec![output(
        queries.pop().unwrap(),
        outcome,
    )])))
}

async fn execute_points(work: Work) -> Result<Output> {
    let mut outputs = Vec::with_capacity(work.queries.len());
    for query in work.queries {
        let (request, credentials) = request(&query)?;
        let mut outcome = match tokio::time::timeout_at(
            work.deadline,
            work.view.read(request, credentials),
        )
        .await
        {
            Ok(outcome) => outcome?,
            Err(_) => timed_out(),
        };
        work.header.outcome(&mut outcome);
        outputs.push(output(query, outcome));
    }
    Ok(Output::Reads(outputs))
}

fn output(query: Query, outcome: crate::mvcc::Outcome) -> Output {
    match query {
        Query::Read(_, _, response) => Output::Read { response, outcome },
        Query::Linear(_, _, id) => Output::Linear { id, outcome },
        _ => unreachable!(),
    }
}
