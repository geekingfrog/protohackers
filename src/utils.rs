use tokio::task::JoinHandle;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type BoxResult<T> = Result<T, BoxError>;

/// make sure to abort a task on drop, so that it's easier to
/// do assertion in tests with less worry about cleaning up
pub(crate) struct AbortHdl<T>(pub(crate) JoinHandle<T>);

impl<T> Drop for AbortHdl<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}
