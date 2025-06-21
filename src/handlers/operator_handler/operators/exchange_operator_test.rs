use std::sync::Arc;

use anyhow::Result;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::error;
use tracing_subscriber;

use crate::{
    handlers::{
        message_handler::{messages, MessageRegistry, Pipe},
        message_router_handler::{MessageRouterHandler, MockSubscriber},
        operator_handler::{operators::requests, OperatorInstanceConfig},
    },
    planner,
};

use super::exchange_operator::ExchangeOperator;

#[tokio::test]
async fn test_exchange_operator_create_transaction() -> Result<()> {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let ct = CancellationToken::new();
    let tt = TaskTracker::new();
    let msg_reg = Arc::new(MessageRegistry::new());

    let op_in_config = OperatorInstanceConfig {
        id: 321,
        query_handler_worker_id: 0,
        query_id: 4321,
        pipeline_id: "pipeline_id".to_string(),
        operator: planner::Operator {
            id: "op-id-1".to_string(),
            plan_id: 1,
            operator_type: planner::OperatorType::Exchange {
                outbound_producer_ids: vec!["op-id-2".to_string()],
                inbound_producer_ids: vec!["op-id-1".to_string()],
                record_queue_configs: vec![planner::ExchangeRecordQueueConfig {
                    producer_id: "op-id-2".to_string(),
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

    let resp_msg = requests::exchange::CreateTransactionRequest::create_transaction_request(
        "talo".to_string(),
        321,
        0,
        &mut pipe,
        msg_reg.clone(),
    )
    .await?;

    assert_eq!(
        resp_msg,
        messages::exchange::CreateTransactionResponse::Ok { transaction_id: 0 }
    );

    ct.cancel();
    tt.close();
    tokio::time::timeout(chrono::Duration::seconds(8).to_std()?, async move {
        tt.wait().await;
    })
    .await?;

    Ok(())
}
