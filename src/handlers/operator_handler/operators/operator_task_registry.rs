use super::{table_funcs, traits::TableFuncTaskBuilder};

pub struct OperatorTaskRegistry {
    table_func_task_builders: Vec<Box<dyn TableFuncTaskBuilder>>,
}

impl OperatorTaskRegistry {
    pub fn new() -> OperatorTaskRegistry {
        OperatorTaskRegistry {
            table_func_task_builders: Vec::new(),
        }
    }

    pub fn add_table_func_task_builder(mut self, bldr: Box<dyn TableFuncTaskBuilder>) -> Self {
        self.table_func_task_builders.push(bldr);
        self
    }

    pub fn get_table_func_task_builders(&self) -> &Vec<Box<dyn TableFuncTaskBuilder>> {
        &self.table_func_task_builders
    }
}

pub fn build_default_operator_task_registry() -> OperatorTaskRegistry {
    OperatorTaskRegistry::new()
        .add_table_func_task_builder(Box::new(table_funcs::ReadFilesTaskBuilder::new()))
}
