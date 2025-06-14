use std::ops::Deref;

use tokio_util::sync::CancellationToken;

pub struct DropCancellationToken {
    pub ct: CancellationToken,
}

impl DropCancellationToken {
    pub fn new(ct: CancellationToken) -> DropCancellationToken {
        DropCancellationToken { ct }
    }
}

impl Drop for DropCancellationToken {
    fn drop(&mut self) {
        self.ct.cancel();
    }
}

impl Deref for DropCancellationToken {
    type Target = CancellationToken;
    fn deref(&self) -> &Self::Target {
        &self.ct
    }
}
