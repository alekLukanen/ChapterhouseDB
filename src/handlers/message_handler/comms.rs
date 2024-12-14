use anyhow::Result;
use tokio::sync::mpsc;

pub struct Pipe<T> {
    sender: mpsc::Sender<T>,
    receiver: mpsc::Receiver<T>,
}

impl<T> Pipe<T>
where
    T: 'static + Send + Sync,
{
    pub fn new(size: usize) -> (Pipe<T>, Pipe<T>) {
        let (tx1, rx1) = mpsc::channel(size);
        let (tx2, rx2) = mpsc::channel(size);
        (
            Pipe {
                sender: tx1,
                receiver: rx2,
            },
            Pipe {
                sender: tx2,
                receiver: rx1,
            },
        )
    }

    pub async fn send(&self, msg: T) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }
}
