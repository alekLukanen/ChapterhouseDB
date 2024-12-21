use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Pipe<T> {
    sender: mpsc::Sender<T>,
    receiver: mpsc::Receiver<T>,
}

impl<T> Pipe<T>
where
    T: 'static + Send + Sync,
{
    /*
    Creates two pipes that can communicate with one another.
    */
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

    /*
    Returns a pipe with the supplied sender and the sender that
    can be used to supply data to the pipe.
    Useful if you need multiple pipes to feed into the same receiver.
     */
    pub fn new_with_existing_sender(
        sender: mpsc::Sender<T>,
        size: usize,
    ) -> (Pipe<T>, mpsc::Sender<T>) {
        let (tx, rx) = mpsc::channel(size);
        (
            Pipe {
                sender,
                receiver: rx,
            },
            tx,
        )
    }

    pub async fn send(&self, msg: T) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn close_receiver(&mut self) {
        self.receiver.close();
    }

    pub fn split(self) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        (self.sender, self.receiver)
    }
}
