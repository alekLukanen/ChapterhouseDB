use anyhow::Result;
use thiserror::Error;
use tokio_util::task::TaskTracker;

#[derive(Debug, Error)]
pub enum RestrictedOperatorTaskTrackerError {
    #[error("already reached max spawn count")]
    AlreadyReachedMaxSpawnCount,
}

pub struct RestrictedOperatorTaskTracker<'a> {
    tt: &'a TaskTracker,
    max_spawn: usize,
    spawned: usize,
}

impl<'a> RestrictedOperatorTaskTracker<'a> {
    pub fn new(tt: &TaskTracker, max_spawn: usize) -> RestrictedOperatorTaskTracker {
        RestrictedOperatorTaskTracker {
            tt,
            max_spawn,
            spawned: 0,
        }
    }
}

impl<'a> RestrictedOperatorTaskTracker<'a> {
    pub fn spawn(
        &mut self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        if self.spawned < self.max_spawn {
            self.tt.spawn(task);
            self.spawned += 1;
            Ok(())
        } else {
            Err(RestrictedOperatorTaskTrackerError::AlreadyReachedMaxSpawnCount.into())
        }
    }
}
