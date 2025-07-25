use anyhow::Result;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, Ident, SelectItem,
    Value, WildcardAdditionalOptions,
};

use super::logical_planner::{LogicalPlan, LogicalPlanNodeType, LogicalPlanner};

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

                let table_node_idx = lp.add_node(
                    LogicalPlanNodeType::Table {
                        alias: None,
                        name: "bikes".to_string(),
                    },
                    false,
                );
                let materialize_node_idx = lp.add_node(
                    LogicalPlanNodeType::Materialize {
                        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_except: None,
                            opt_ilike: None,
                            opt_rename: None,
                            opt_exclude: None,
                            opt_replace: None,
                        })],
                    },
                    true,
                );

                lp.connect(table_node_idx, materialize_node_idx);

                Some(lp)
            }),
        },
        TestCase {
            case_name: "simple-select-where".to_string(),
            query: "select * from bikes where size='small'".to_string(),
            expected_plan: Box::new(|| -> Option<LogicalPlan> {
                let mut lp = LogicalPlan::new();

                let table_node_idx = lp.add_node(
                    LogicalPlanNodeType::Table {
                        alias: None,
                        name: "bikes".to_string(),
                    },
                    false,
                );
                let filter_node_idx = lp.add_node(
                    LogicalPlanNodeType::Filter {
                        expr: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "size".to_string(),
                                quote_style: None,
                            })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(Value::SingleQuotedString(
                                "small".to_string(),
                            ))),
                        },
                    },
                    false,
                );
                let materialize_node_idx = lp.add_node(
                    LogicalPlanNodeType::Materialize {
                        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_except: None,
                            opt_ilike: None,
                            opt_rename: None,
                            opt_exclude: None,
                            opt_replace: None,
                        })],
                    },
                    true,
                );

                lp.connect(table_node_idx, filter_node_idx);
                lp.connect(filter_node_idx, materialize_node_idx);

                Some(lp)
            }),
        },
        TestCase {
            case_name: "simple-select-table-func".to_string(),
            query: "select * from read_files('data/path/*.csv', connection=>'big_s3') files"
                .to_string(),
            expected_plan: Box::new(|| -> Option<LogicalPlan> {
                let mut lp = LogicalPlan::new();

                let table_node_idx = lp.add_node(
                    LogicalPlanNodeType::TableFunc {
                        alias: Some("files".to_string()),
                        name: "read_files".to_string(),
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString("data/path/*.csv".to_string()),
                            ))),
                            FunctionArg::Named {
                                name: Ident {
                                    value: "connection".to_string(),
                                    quote_style: None,
                                },
                                arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                                    "big_s3".to_string(),
                                ))),
                                operator: FunctionArgOperator::RightArrow,
                            },
                        ],
                    },
                    false,
                );
                let materialize_node_idx = lp.add_node(
                    LogicalPlanNodeType::Materialize {
                        fields: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_except: None,
                            opt_ilike: None,
                            opt_rename: None,
                            opt_exclude: None,
                            opt_replace: None,
                        })],
                    },
                    true,
                );

                lp.connect(table_node_idx, materialize_node_idx);

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
