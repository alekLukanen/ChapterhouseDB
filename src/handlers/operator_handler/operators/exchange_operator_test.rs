use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::error;

use crate::{
    handlers::{
        message_handler::{MessageRegistry, Pipe},
        message_router_handler::{MessageRouterHandler, MessageRouterState, MockSubscriber},
        operator_handler::OperatorInstanceConfig,
    },
    planner,
};

use super::exchange_operator::ExchangeOperator;

#[tokio::test]
async fn test_exchange_operator_create_transaction() -> Result<()> {
    let ct = CancellationToken::new();
    let tt = TaskTracker::new();
    let msg_reg = Arc::new(MessageRegistry::new());

    let op_in_config = OperatorInstanceConfig {
        id: 321,
        query_handler_worker_id: 3322,
        query_id: 4321,
        pipeline_id: "pipeline_id".to_string(),
        operator: planner::Operator {
            id: "op-id-1".to_string(),
            plan_id: 1,
            operator_type: planner::OperatorType::Exchange {
                outbound_producer_ids: vec!["op-id-2".to_string()],
                inbound_producer_ids: vec!["op-id-1".to_string()],
                record_queue_configs: vec![planner::ExchangeRecordQueueConfig {
                    producer_id: "op-id-1".to_string(),
                    queue_name: "default".to_string(),
                    sampling_method: planner::ExchangeRecordQueueSamplingMethod::All,
                }],
            },
            compute: planner::OperatorCompute {
                cpu_in_thousandths: 10,
                memory_in_mib: 10,
                instances: 1,
            },
        },
    };

    let (mut msg_router, msg_router_state) =
        MessageRouterHandler::new(0, Pipe::new(100).0, msg_reg.clone());
    let mut exchange_op =
        ExchangeOperator::new(op_in_config, msg_router_state.clone(), msg_reg.clone()).await?;

    let tt_ct = ct.child_token();
    tt.spawn(async move {
        if let Err(err) = msg_router.async_main(tt_ct).await {
            error!("{}", err);
        }
    });

    let tt_ct = ct.child_token();
    tt.spawn(async move {
        if let Err(err) = exchange_op.async_main(tt_ct).await {
            error!("{}", err);
        }
    });

    let (mut pipe, sender) =
        Pipe::new_with_existing_sender(msg_router_state.lock().await.sender(), 10);
    pipe.set_sent_from_query_id(4321);
    pipe.set_sent_from_operation_id(6767);
    let subscriber = Box::new(MockSubscriber::new(sender));

    msg_router_state
        .lock()
        .await
        .add_internal_subscriber(subscriber, 6767);

    Ok(())
}
