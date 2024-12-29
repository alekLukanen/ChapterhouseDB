use super::{
    table_funcs,
    traits::{TableFuncSyntaxValidator, TableFuncTaskBuilder},
};

struct TableFuncTaskDef {
    builder: Box<dyn TableFuncTaskBuilder>,
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
        builder: Box<dyn TableFuncTaskBuilder>,
        syntax_validator: Box<dyn TableFuncSyntaxValidator>,
    ) -> Self {
        self.table_funcs.push(TableFuncTaskDef {
            builder,
            syntax_validator,
        });
        self
    }

    pub fn find_table_func_task_builder(
        &self,
        func_name: &String,
    ) -> Option<&Box<dyn TableFuncTaskBuilder>> {
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
        Box::new(table_funcs::ReadFilesTaskBuilder::new()),
        Box::new(table_funcs::ReadFilesSyntaxValidator::new()),
    )
}
