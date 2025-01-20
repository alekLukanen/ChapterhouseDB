use std::collections::VecDeque;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use super::{Message, MessageName};

const MAX_MSG_QUEUE_LENGTH: usize = 100;

#[derive(Debug, Error)]
pub enum PipeError {
    #[error("timed out waiting for message to send")]
    TimedOutWaitingForMessageToSend,
    #[error("timed out waiting for message by name: {0}")]
    TimedOutWaitingForMessageByName(MessageName),
    #[error("queue has reach max capacity: {0}")]
    QueueHasReachedMaxCapacity(usize),
}

#[derive(Debug)]
pub struct Pipe {
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
    sent_from_query_id: Option<u128>,
    sent_from_operation_id: Option<u128>,
    msg_queue: VecDeque<Message>,
    max_msg_queue_length: usize,
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
                max_msg_queue_length: MAX_MSG_QUEUE_LENGTH,
            },
            Pipe {
                sender: tx2,
                receiver: rx1,
                sent_from_query_id: None,
                sent_from_operation_id: None,
                msg_queue: VecDeque::new(),
                max_msg_queue_length: MAX_MSG_QUEUE_LENGTH,
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
                max_msg_queue_length: MAX_MSG_QUEUE_LENGTH,
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
        if self.msg_queue.len() > 0 {
            return self.msg_queue.pop_front();
        }
        self.receiver.recv().await
    }

    pub async fn recv_filter(
        &mut self,
        msg_name: MessageName,
        max_wait: chrono::Duration,
    ) -> Result<Option<Message>> {
        // TODO: implement message queue filter pop

        tokio::time::timeout(max_wait.to_std()?, async {
            loop {
                match self.receiver.recv().await {
                    Some(msg) => {
                        if msg.msg.msg_name() == msg_name {
                            return Ok(Some(msg));
                        } else if self.msg_queue.len() < self.max_msg_queue_length {
                            self.msg_queue.push_back(msg);
                        } else {
                            return Err(PipeError::QueueHasReachedMaxCapacity(
                                self.max_msg_queue_length,
                            )
                            .into());
                        }
                    }
                    None => {
                        return Ok(None);
                    }
                }
            }
        })
        .await
        .unwrap_or(Err(
            PipeError::TimedOutWaitingForMessageByName(msg_name).into()
        ))
    }

    pub fn close_receiver(&mut self) {
        self.receiver.close();
    }

    pub fn split(self) -> (mpsc::Sender<Message>, mpsc::Receiver<Message>) {
        (self.sender, self.receiver)
    }
}
