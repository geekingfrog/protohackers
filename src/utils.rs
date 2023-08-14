use tokio::{task::JoinHandle, sync::oneshot};

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type BoxResult<T> = Result<T, BoxError>;

/// send something on a oneshot and ignores any error
/// typically used to transmit a response to a client in another task
/// and if the client goes away before the response can be transmitted
/// we don't really care
pub(crate) fn yolo_send<T>(tx: oneshot::Sender<T>, x: T) {
    if let Err(_) = tx.send(x) {}
}
/// make sure to abort a task on drop, so that it's easier to
/// do assertion in tests with less worry about cleaning up
pub(crate) struct AbortHdl<T>(pub(crate) JoinHandle<T>);

impl<T> Drop for AbortHdl<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}
