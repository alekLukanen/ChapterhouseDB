use anyhow::Result;
use tokio_util::task::TaskTracker;

pub struct RestrictedOperatorTaskTracker<'a> {
    tt: &'a TaskTracker,
    max_spawn: usize,
}

impl<'a> RestrictedOperatorTaskTracker<'a> {
    pub fn new(tt: &TaskTracker, max_spawn: usize) -> RestrictedOperatorTaskTracker {
        RestrictedOperatorTaskTracker { tt, max_spawn }
    }
}

impl<'a> RestrictedOperatorTaskTracker<'a> {
    pub fn spawn(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        self.tt.spawn(task);
        Ok(())
    }
}
