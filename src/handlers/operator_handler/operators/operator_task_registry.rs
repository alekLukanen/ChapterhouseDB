use crate::planner;

use super::{
    table_func_tasks,
    traits::{TableFuncSyntaxValidator, TaskBuilder},
};
use anyhow::Result;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OperatorTaskRegistryError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

struct TableFuncTaskDef {
    builder: Box<dyn TaskBuilder>,
    syntax_validator: Box<dyn TableFuncSyntaxValidator>,
}

pub struct OperatorTaskRegistry {
    table_funcs: Vec<TableFuncTaskDef>,
}

impl OperatorTaskRegistry {
    pub fn new() -> OperatorTaskRegistry {
        OperatorTaskRegistry {
            table_funcs: Vec::new(),
        }
    }

    pub fn add_table_func_task_builder(
        mut self,
        builder: Box<dyn TaskBuilder>,
        syntax_validator: Box<dyn TableFuncSyntaxValidator>,
    ) -> Self {
        self.table_funcs.push(TableFuncTaskDef {
            builder,
            syntax_validator,
        });
        self
    }

    pub fn find_task_builder(
        &self,
        task: &planner::OperatorTask,
    ) -> Result<Option<&Box<dyn TaskBuilder>>> {
        match task {
            planner::OperatorTask::TableFunc { func_name, .. } => {
                Ok(self.find_table_func_task_builder_by_name(func_name))
            }
            planner::OperatorTask::Table { .. } => Err(OperatorTaskRegistryError::NotImplemented(
                format!("find task builder for OperatorTask type {}", task.name()),
            )
            .into()),
            planner::OperatorTask::Filter { .. } => Err(OperatorTaskRegistryError::NotImplemented(
                format!("find task builder for OperatorTask type {}", task.name()),
            )
            .into()),
            planner::OperatorTask::MaterializeFile { .. } => {
                Err(OperatorTaskRegistryError::NotImplemented(format!(
                    "find task builder for OperatorTask type {}",
                    task.name()
                ))
                .into())
            }
        }
    }

    pub fn find_table_func_task_builder_by_name(
        &self,
        func_name: &String,
    ) -> Option<&Box<dyn TaskBuilder>> {
        let def = self
            .table_funcs
            .iter()
            .find(|item| item.syntax_validator.implements_func_name() == *func_name);
        if let Some(def) = def {
            Some(&def.builder)
        } else {
            None
        }
    }
}

pub fn build_default_operator_task_registry() -> OperatorTaskRegistry {
    OperatorTaskRegistry::new().add_table_func_task_builder(
        Box::new(table_func_tasks::ReadFilesTaskBuilder::new()),
        Box::new(table_func_tasks::ReadFilesSyntaxValidator::new()),
    )
}
