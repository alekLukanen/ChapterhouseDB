use anyhow::Result;
use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

use crate::planner::logical_planner::LogicalPlanner;

use super::logical_planner::{LogicalPlan, PlanNode, Stage, StageType};

#[test]
fn test_simple_logical_plans() -> Result<()> {
    struct TestCase {
        case_name: String,
        query: String,
        expected_plan: Box<dyn Fn() -> Option<LogicalPlan>>,
    }

    let test_cases = vec![
        TestCase {
            case_name: "simple-select".to_string(),
            query: "select * from bikes".to_string(),
            expected_plan: Box::new(|| -> Option<LogicalPlan> {
                let mut lp = LogicalPlan::new();

                let table_source_stage = Stage::new(StageType::TableSource, 0);
                let materialize_stage = Stage::new(StageType::Materialize, 2);

                lp.add_node(
                    PlanNode::Table {
                        alias: None,
                        name: "bikes".to_string(),
                    },
                    table_source_stage.clone(),
                );
                lp.add_node(
                    PlanNode::Materialize {
                        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_except: None,
                            opt_ilike: None,
                            opt_rename: None,
                            opt_exclude: None,
                            opt_replace: None,
                        })],
                    },
                    materialize_stage.clone(),
                );

                lp.connect_stages(table_source_stage.clone(), materialize_stage.clone());

                Some(lp)
            }),
        },
        TestCase {
            case_name: "simple-select-where".to_string(),
            query: "select * from bikes where size='small'".to_string(),
            expected_plan: Box::new(|| -> Option<LogicalPlan> {
                let mut lp = LogicalPlan::new();

                let table_source_stage = Stage::new(StageType::TableSource, 0);
                let materialize_stage = Stage::new(StageType::Materialize, 2);

                lp.add_node(
                    PlanNode::Table {
                        alias: None,
                        name: "bikes".to_string(),
                    },
                    table_source_stage.clone(),
                );
                lp.add_node(
                    PlanNode::Materialize {
                        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_except: None,
                            opt_ilike: None,
                            opt_rename: None,
                            opt_exclude: None,
                            opt_replace: None,
                        })],
                    },
                    materialize_stage.clone(),
                );

                lp.connect_stages(table_source_stage.clone(), materialize_stage.clone());

                Some(lp)
            }),
        },
    ];

    for test_case in test_cases {
        println!("test case: {}", test_case.case_name);
        let mut planner = LogicalPlanner::new(test_case.query);
        let lp = planner.build()?;
        let expected_lp = (test_case.expected_plan)();

        if let Some(expected_lp_val) = expected_lp {
            assert_eq!(expected_lp_val, lp);
        } else {
            panic!()
        }
    }

    Ok(())
}
