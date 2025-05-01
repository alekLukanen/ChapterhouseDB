#[macro_export]
macro_rules! retry_request {
    ($f:expr, $retries:expr, $interval_in_millis:expr) => {{
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..($retries + 1) {
            match $f.await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    last_err = Some(err);

                    tokio::time::sleep(std::time::Duration::from_millis(std::cmp::min(
                        (retry_idx + 1) * $interval_in_millis as u64,
                        1,
                    )))
                    .await;
                }
            }
        }
        Err(last_err.unwrap().context("failed after retries"))
    }};
}
pub use retry_request;
