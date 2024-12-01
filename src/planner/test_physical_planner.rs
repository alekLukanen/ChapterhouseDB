use anyhow::{Error, Result};
use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanner};
use crate::planner::physical_planner::{
    DataFormat, Operation, OperationTask, PhysicalPlan, PhysicalPlanner, Pipeline, TaskType,
};

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

            // ensure all plan nodes have a corresponding physical operation
            for plan_node_id in plan_node_ids {
                if !query_pipeline.has_operations_for_plan_id(plan_node_id.clone()) {
                    return Err(Error::msg(format!(
                        "plan_node_id {} is missing physical operations",
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
