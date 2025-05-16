use crate::planner::{self, DataFormat};

use super::{
    filter_tasks, materialize_tasks, partition_tasks, table_func_tasks,
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
    #[error("filter task builder already set")]
    FilterTaskBuilderAlreadySet,
    #[error("task func task builder already added for function: {0}")]
    TaskFuncTaskBuilderAlreadyAddedForFunction(String),
    #[error("partition task builder already set")]
    PartitionTaskBuilderAlreadySet,
    #[error("sort task builder already set")]
    SortTaskBuilderAlreadySet,
}

struct TableFuncTaskDef {
    builder: Box<dyn TaskBuilder>,
    syntax_validator: Box<dyn TableFuncSyntaxValidator>,
}

struct MaterializeFileTaskDef {
    builder: Box<dyn TaskBuilder>,
    data_formats: Vec<planner::DataFormat>,
}

struct FilterTaskDef {
    builder: Box<dyn TaskBuilder>,
}

struct PartitionTaskDef {
    builder: Box<dyn TaskBuilder>,
}

struct SortTaskDef {
    builder: Box<dyn TaskBuilder>,
}

pub struct OperatorTaskRegistry {
    table_func_tasks: Vec<TableFuncTaskDef>,
    materialize_files_task: Option<MaterializeFileTaskDef>,
    filter_task: Option<FilterTaskDef>,
    partition_task: Option<PartitionTaskDef>,
    sort_task: Option<SortTaskDef>,
}

impl OperatorTaskRegistry {
    pub fn new() -> OperatorTaskRegistry {
        OperatorTaskRegistry {
            table_func_tasks: Vec::new(),
            materialize_files_task: None,
            filter_task: None,
            partition_task: None,
            sort_task: None,
        }
    }

    pub fn add_partition_task_builder(mut self, builder: Box<dyn TaskBuilder>) -> Result<Self> {
        if self.partition_task.is_some() {
            return Err(OperatorTaskRegistryError::PartitionTaskBuilderAlreadySet.into());
        }
        self.partition_task = Some(PartitionTaskDef { builder });
        Ok(self)
    }

    pub fn add_sort_task_builder(mut self, builder: Box<dyn TaskBuilder>) -> Result<Self> {
        if self.sort_task.is_some() {
            return Err(OperatorTaskRegistryError::SortTaskBuilderAlreadySet.into());
        }
        self.sort_task = Some(SortTaskDef { builder });
        Ok(self)
    }

    pub fn add_filter_task_builder(mut self, builder: Box<dyn TaskBuilder>) -> Result<Self> {
        if self.filter_task.is_some() {
            return Err(OperatorTaskRegistryError::FilterTaskBuilderAlreadySet.into());
        }
        self.filter_task = Some(FilterTaskDef { builder });
        Ok(self)
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
            planner::OperatorTask::Filter { .. } => {
                if let Some(filter_task) = &self.filter_task {
                    Ok(Some(&filter_task.builder))
                } else {
                    Ok(None)
                }
            }
            planner::OperatorTask::Partition { .. } => {
                Err(OperatorTaskRegistryError::NotImplemented(format!(
                    "find task builder for OperatorTask type {}",
                    task.name()
                ))
                .into())
            }
            planner::OperatorTask::Sort { .. } => Err(OperatorTaskRegistryError::NotImplemented(
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
        .add_filter_task_builder(Box::new(filter_tasks::FilterTaskBuilder::new()))?
        .add_materialize_files_builder(
            Box::new(materialize_tasks::MaterializeFilesTaskBuilder::new()),
            vec![DataFormat::Parquet],
        )?
        .add_partition_task_builder(Box::new(partition_tasks::PartitionTaskBuilder::new()))?;
    Ok(reg)
}
