use std::collections::VecDeque;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::field::debug;

use super::{Message, MessageName};

#[derive(Debug, Error)]
pub enum PipeError {
    #[error("timed out waiting for message to send")]
    TimedOutWaitingForMessageToSend,
}

#[derive(Debug)]
pub struct Pipe {
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
    sent_from_query_id: Option<u128>,
    sent_from_operation_id: Option<u128>,
    msg_queue: VecDeque<Message>,
}

impl Pipe {
    /*
    Creates two pipes that can communicate with one another.
    */
    pub fn new(size: usize) -> (Pipe, Pipe) {
        let (tx1, rx1) = mpsc::channel(size);
        let (tx2, rx2) = mpsc::channel(size);
        (
            Pipe {
                sender: tx1,
                receiver: rx2,
                sent_from_query_id: None,
                sent_from_operation_id: None,
                msg_queue: VecDeque::new(),
            },
            Pipe {
                sender: tx2,
                receiver: rx1,
                sent_from_query_id: None,
                sent_from_operation_id: None,
                msg_queue: VecDeque::new(),
            },
        )
    }

    /*
    Returns a pipe with the supplied sender and the sender that
    can be used to supply data to the pipe.
    Useful if you need multiple pipes to feed into the same receiver.
     */
    pub fn new_with_existing_sender(
        sender: mpsc::Sender<Message>,
        size: usize,
    ) -> (Pipe, mpsc::Sender<Message>) {
        let (tx, rx) = mpsc::channel(size);
        (
            Pipe {
                sender,
                receiver: rx,
                sent_from_query_id: None,
                sent_from_operation_id: None,
                msg_queue: VecDeque::new(),
            },
            tx,
        )
    }

    pub fn set_sent_from_query_id(&mut self, _id: u128) -> &Self {
        self.sent_from_query_id = Some(_id);
        self
    }

    pub fn set_sent_from_operation_id(&mut self, _id: u128) -> &Self {
        self.sent_from_operation_id = Some(_id);
        self
    }

    pub async fn send(&self, msg: Message) -> Result<()> {
        let mut msg = msg;
        if let Some(id) = self.sent_from_query_id {
            msg = msg.set_sent_from_query_id(id);
        }
        if let Some(id) = self.sent_from_operation_id {
            msg = msg.set_sent_from_operation_id(id)
        }
        tokio::select! {
            _ = self.sender.send(msg) => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                return Err(PipeError::TimedOutWaitingForMessageToSend.into());
            }
        }
        Ok(())
    }

    pub async fn send_all(&self, msgs: Vec<Message>) -> Result<()> {
        for msg in msgs {
            self.sender.send(msg).await?;
        }
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }

    pub async fn recv_filter(
        &mut self,
        msg_name: MessageName,
        max_wait: chrono::Duration,
    ) -> Result<Option<Message>> {
        let timer = tokio::time::sleep(max_wait.to_std()?);
        loop {
            tokio::select! {
                msg = self.receiver.recv() => {

                }
                _ = timer => {
                }
            }
            let msg = self.receiver.recv().await;
            match msg {
                Some(msg) => {
                    if msg.msg.msg_name() == msg_name {
                        return Ok(Some(msg));
                    } else {
                        self.msg_queue.push_back(msg);
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }

    pub fn close_receiver(&mut self) {
        self.receiver.close();
    }

    pub fn split(self) -> (mpsc::Sender<Message>, mpsc::Receiver<Message>) {
        (self.sender, self.receiver)
    }
}
