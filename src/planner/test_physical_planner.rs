use anyhow::{Error, Result};
use sqlparser::ast::{Expr, SelectItem, Value, WildcardAdditionalOptions};

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanner};
use crate::planner::physical_planner::{
    DataFormat, Operator, OperatorCompute, OperatorTask, OperatorType, PhysicalPlan,
    PhysicalPlanner, Pipeline,
};

use super::logical_planner::LogicalPlanNodeType;

#[test]
fn test_simple_physical_plans() -> Result<()> {
    struct TestCase {
        case_name: String,
        logical_plan: Box<dyn Fn() -> Result<LogicalPlan>>,
        plan_matchs_expected: Box<dyn Fn(&LogicalPlan, &PhysicalPlan) -> Result<()>>,
    }

    let test_cases = vec![TestCase {
        case_name: "select-with-filter-and-table-func".to_string(),
        logical_plan: Box::new(|| -> Result<LogicalPlan> {
            let query = "select * from read_files('data/path/*.parquet') where size = 'medium'";
            let res = LogicalPlanner::new(query.to_string()).build()?;
            Ok(res)
        }),
        plan_matchs_expected: Box::new(|lp, pp| -> Result<()> {
            let plan_node_ids = &lp.get_all_node_ids();
            let mut pipelines = pp.get_pipelines();

            assert_eq!(1, pipelines.len());

            let query_pipeline = &pipelines.remove(0);

            println!("physical plan: {:?}", pp);

            // ensure all plan nodes have a corresponding physical operator
            for plan_node_id in plan_node_ids {
                if !query_pipeline.has_operators_for_plan_id(plan_node_id.clone()) {
                    return Err(Error::msg(format!(
                        "plan_node_id {} is missing physical operators",
                        plan_node_id
                    )));
                }
            }

            Ok(())
        }),
    }];

    for test_case in test_cases {
        println!("test case: {}", test_case.case_name);
        let lp = &(test_case.logical_plan)()?;
        let mut planner = PhysicalPlanner::new(lp.clone());
        let pp = &planner.build()?;

        (test_case.plan_matchs_expected)(lp, pp)?;
    }

    Ok(())
}

#[test]
fn test_build_materialize_operators() -> Result<()> {
    let query = "select * from read_files('data/path/*.parquet') where true";
    let logical_plan = LogicalPlanner::new(query.to_string()).build()?;
    let mut pipeline = Pipeline::new("pipeline_0".to_string());

    let plan_nodes = logical_plan.get_all_nodes();
    let materialize_node = if let Some(mat_node) = plan_nodes
        .iter()
        .find(|&item| matches!(item.node, LogicalPlanNodeType::Materialize { .. }))
    {
        mat_node.clone()
    } else {
        return Err(Error::msg("unable to find materialize node for test prep"));
    };

    let filter_node = if let Some(filter_node) = plan_nodes
        .iter()
        .find(|&item| matches!(item.node, LogicalPlanNodeType::Filter { .. }))
    {
        filter_node.clone()
    } else {
        return Err(Error::msg("unable to find filter node for test prep"));
    };

    // add the inbound filter exchange
    let ref filter_exchange = Operator {
        id: format!("operator_p{}_exchange", filter_node.id),
        plan_id: filter_node.id,
        operator_type: OperatorType::Exchange {
            task: OperatorTask::Filter {
                expr: Expr::Value(Value::Boolean(true)),
            },
            outbound_producer_ids: vec![format!(
                "operator_p{}_exchange",
                materialize_node.id.clone()
            )],
            inbound_producer_ids: vec![format!("operator_p{}_producer", filter_node.id.clone())],
        },
        compute: OperatorCompute {
            instances: 1,
            cpu_in_thousandths: 1000,
            memory_in_mib: 128,
        },
    };
    pipeline.add_operator(filter_exchange.clone());

    let mut physical_planner = PhysicalPlanner::new(logical_plan);
    let mut operations = physical_planner.build_materialize_operators(&materialize_node)?;

    assert_eq!(2, operations.len());

    let ref expected_task_type = OperatorTask::MaterializeFiles {
        data_format: DataFormat::Parquet,
        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
            opt_except: None,
            opt_ilike: None,
            opt_rename: None,
            opt_exclude: None,
            opt_replace: None,
        })],
    };
    let expected_producer = Operator {
        id: format!("operator_p{}_producer", materialize_node.id),
        plan_id: materialize_node.id,
        operator_type: OperatorType::Producer {
            task: expected_task_type.clone(),
            outbound_exchange_id: format!("operator_p{}_exchange", materialize_node.id.clone()),
            inbound_exchange_ids: vec![format!("operator_p{}_exchange", filter_node.id.clone())],
        },
        compute: OperatorCompute {
            instances: 1,
            cpu_in_thousandths: 1000,
            memory_in_mib: 512,
        },
    };
    let expected_exchange = Operator {
        id: format!("operator_p{}_exchange", materialize_node.id),
        plan_id: materialize_node.id,
        operator_type: OperatorType::Exchange {
            task: expected_task_type.clone(),
            outbound_producer_ids: vec![],
            inbound_producer_ids: vec![expected_producer.id.clone()],
        },
        compute: OperatorCompute {
            instances: 1,
            cpu_in_thousandths: 200,
            memory_in_mib: 128,
        },
    };
    let mut expected_operators = vec![expected_producer, expected_exchange];

    operations.sort();
    expected_operators.sort();

    assert_eq!(expected_operators, operations);

    Ok(())
}
