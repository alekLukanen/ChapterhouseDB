use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{MessageConsumer, MessageReceiver, Subscriber};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

#[derive(Debug)]
pub struct ProducerOperator {
    operator_instance_config: OperatorInstanceConfig,
    router_pipe: Pipe<Message>,
    task_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl ProducerOperator {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        router_sender: mpsc::Sender<Message>,
        task_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
    ) -> ProducerOperator {
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

        ProducerOperator {
            operator_instance_config: op_in_config,
            router_pipe: pipe,
            task_pipe,
            sender,
            msg_reg,
        }
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(ProducerOperatorSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
            operator_instance_id: self.operator_instance_config.id.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!(
            "closing operator producer for instance {}",
            self.operator_instance_config.id
        );

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct ProducerOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    operator_instance_id: u128,
}

impl Subscriber for ProducerOperatorSubscriber {}

impl MessageConsumer for ProducerOperatorSubscriber {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        if let Some(route_to_operation_id) = msg.route_to_operation_id {
            if route_to_operation_id == self.operator_instance_id {
                return true;
            }
        } else {
            return false;
        }
        false
    }
}

impl MessageReceiver for ProducerOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
