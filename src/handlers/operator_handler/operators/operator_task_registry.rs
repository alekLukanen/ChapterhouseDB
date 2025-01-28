use crate::planner::{self, DataFormat};

use super::{
    materialize_tasks, table_func_tasks,
    traits::{TableFuncSyntaxValidator, TaskBuilder},
};
use anyhow::Result;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OperatorTaskRegistryError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("materialize file task builder already set")]
    MaterializeFileTaskBuilderAlreadySet,
    #[error("task func task builder already added for function: {0}")]
    TaskFuncTaskBuilderAlreadyAddedForFunction(String),
}

struct TableFuncTaskDef {
    builder: Box<dyn TaskBuilder>,
    syntax_validator: Box<dyn TableFuncSyntaxValidator>,
}

struct MaterializeFileTaskDef {
    builder: Box<dyn TaskBuilder>,
    data_formats: Vec<planner::DataFormat>,
}

pub struct OperatorTaskRegistry {
    table_func_tasks: Vec<TableFuncTaskDef>,
    materialize_files_task: Option<MaterializeFileTaskDef>,
}

impl OperatorTaskRegistry {
    pub fn new() -> OperatorTaskRegistry {
        OperatorTaskRegistry {
            table_func_tasks: Vec::new(),
            materialize_files_task: None,
        }
    }

    pub fn add_materialize_files_builder(
        mut self,
        builder: Box<dyn TaskBuilder>,
        data_formats: Vec<planner::DataFormat>,
    ) -> Result<Self> {
        if self.materialize_files_task.is_some() {
            return Err(OperatorTaskRegistryError::MaterializeFileTaskBuilderAlreadySet.into());
        }
        self.materialize_files_task = Some(MaterializeFileTaskDef {
            builder,
            data_formats,
        });
        Ok(self)
    }

    pub fn add_table_func_task_builder(
        mut self,
        builder: Box<dyn TaskBuilder>,
        syntax_validator: Box<dyn TableFuncSyntaxValidator>,
    ) -> Result<Self> {
        if self.table_func_tasks.iter().any(|task| {
            task.syntax_validator.implements_func_name() == syntax_validator.implements_func_name()
        }) {
            return Err(
                OperatorTaskRegistryError::TaskFuncTaskBuilderAlreadyAddedForFunction(
                    syntax_validator.implements_func_name(),
                )
                .into(),
            );
        }
        self.table_func_tasks.push(TableFuncTaskDef {
            builder,
            syntax_validator,
        });
        Ok(self)
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
            planner::OperatorTask::MaterializeFiles { data_format, .. } => {
                if let Some(materialize_files_task) = &self.materialize_files_task {
                    if materialize_files_task
                        .data_formats
                        .iter()
                        .find(|item| *item == data_format)
                        .is_some()
                    {
                        Ok(Some(&materialize_files_task.builder))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn find_table_func_task_builder_by_name(
        &self,
        func_name: &String,
    ) -> Option<&Box<dyn TaskBuilder>> {
        let def = self
            .table_func_tasks
            .iter()
            .find(|item| item.syntax_validator.implements_func_name() == *func_name);
        if let Some(def) = def {
            Some(&def.builder)
        } else {
            None
        }
    }
}

pub fn build_default_operator_task_registry() -> Result<OperatorTaskRegistry> {
    let reg = OperatorTaskRegistry::new()
        .add_table_func_task_builder(
            Box::new(table_func_tasks::ReadFilesTaskBuilder::new()),
            Box::new(table_func_tasks::ReadFilesSyntaxValidator::new()),
        )?
        .add_materialize_files_builder(
            Box::new(materialize_tasks::MaterializeFilesTaskBuilder::new()),
            vec![DataFormat::Parquet],
        )?;
    Ok(reg)
}
