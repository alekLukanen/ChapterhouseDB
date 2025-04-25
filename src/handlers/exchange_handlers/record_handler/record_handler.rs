use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    operator_handler::operators::requests,
};

pub struct RecordHandler<'a> {
    operator_id: String,
    input_exchange_operator_id: String,
    output_exchange_operator_id: String,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> RecordHandler<'a> {
    pub fn new(
        operator_id: String,
        input_exchange_operator_id: String,
        output_exchange_operator_id: String,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> RecordHandler<'a> {
        RecordHandler {
            operator_id,
            input_exchange_operator_id,
            output_exchange_operator_id,
            pipe,
            msg_reg,
        }
    }

    pub fn next_record(&mut self) {}

    pub fn complete_record(&mut self) {}

    /*
    async fn find_inbound_exchange(&mut self, ct: CancellationToken) -> Result<()> {
        let ref mut operator_pipe = self.pipe;
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            &self.operator_instance_config,
            operator_pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        if resp.len() != 1 {
                            return Err(FilterTaskError::MoreThanOneExchangeIsCurrentlyNotImplemented.into());
                        }
                        let resp = resp.get(0).unwrap();
                        self.inbound_exchange_operator_instance_id = Some(resp.exchange_operator_instance_id);
                        self.inbound_exchange_worker_id = Some(resp.exchange_worker_id);
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            _ = ct.cancelled() => {
                Ok(())
            }
        }
    }

    async fn find_outbound_exchange(&mut self, ct: CancellationToken) -> Result<()> {
        let ref mut operator_pipe = self.operator_pipe;
        let req = requests::IdentifyExchangeRequest::request_outbound_exchange(
            &self.operator_instance_config,
            operator_pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        self.outbound_exchange_operator_instance_id = Some(resp.exchange_operator_instance_id);
                        self.outbound_exchange_worker_id = Some(resp.exchange_worker_id);
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            _ = ct.cancelled() => {
                Ok(())
            }
        }
    }
    */
}
