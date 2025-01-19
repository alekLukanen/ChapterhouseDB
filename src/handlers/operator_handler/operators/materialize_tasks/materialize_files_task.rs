use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, error};

use crate::handlers::{
    message_handler::{MessageName, MessageRegistry, Ping, Pipe, QueryHandlerRequests},
    message_router_handler::MessageConsumer,
    operator_handler::{
        operator_handler_state::OperatorInstanceConfig,
        operators::{
            operator_task_trackers::RestrictedOperatorTaskTracker, traits::TaskBuilder,
            ConnectionRegistry,
        },
    },
};

use super::config::MaterializeFilesConfig;

#[derive(Debug)]
struct MaterializeFilesTask {
    operator_instance_config: OperatorInstanceConfig,
    materialize_file_config: MaterializeFilesConfig,

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,

    exchange_worker_id: Option<u128>,
    exchange_operator_instance_id: Option<u128>,
    record_id: u64,
}

impl MaterializeFilesTask {
    fn new(
        op_in_config: OperatorInstanceConfig,
        materialize_file_config: MaterializeFilesConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
    ) -> MaterializeFilesTask {
        MaterializeFilesTask {
            operator_instance_config: op_in_config,
            materialize_file_config,
            operator_pipe,
            msg_reg,
            conn_reg,
            exchange_worker_id: None,
            exchange_operator_instance_id: None,
            record_id: 0,
        }
    }

    fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(MaterializeFilesConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    async fn async_main(&mut self, ct: tokio_util::sync::CancellationToken) -> Result<()> {
        debug!("materialize_files_task.async_main()");

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Table Func Producer Builder

#[derive(Debug, Clone)]
pub struct MaterializeFilesTaskBuilder {}

impl MaterializeFilesTaskBuilder {
    pub fn new() -> MaterializeFilesTaskBuilder {
        MaterializeFilesTaskBuilder {}
    }
}

impl TaskBuilder for MaterializeFilesTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: tokio_util::sync::CancellationToken,
    ) -> Result<(tokio::sync::oneshot::Receiver<()>, Box<dyn MessageConsumer>)> {
        let mat_files_config = MaterializeFilesConfig::try_from(&op_in_config)?;
        let mut op = MaterializeFilesTask::new(
            op_in_config,
            mat_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
        );

        let consumer = op.consumer();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                error!("{:?}", err);
            }
            if let Err(err) = tx.send(()) {
                error!("{:?}", err);
            }
        })?;

        Ok((rx, consumer))
    }
}

//////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct MaterializeFilesConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for MaterializeFilesConsumer {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        match msg.msg.msg_name() {
            // used to find the exchange
            MessageName::Ping => match self.msg_reg.try_cast_msg::<Ping>(msg) {
                Ok(Ping::Ping) => false,
                Ok(Ping::Pong) => true,
                Err(err) => {
                    error!("{:?}", err);
                    false
                }
            },
            MessageName::QueryHandlerRequests => {
                match self.msg_reg.try_cast_msg::<QueryHandlerRequests>(msg) {
                    Ok(QueryHandlerRequests::ListOperatorInstancesResponse { .. }) => true,
                    Ok(QueryHandlerRequests::ListOperatorInstancesRequest { .. }) => false,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                }
            }
            // used ...
            _ => false,
        }
    }
}
