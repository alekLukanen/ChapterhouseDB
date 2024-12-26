use anyhow::Result;

use crate::handlers::operator_handler::operator_handler_state::OperatorInstance;

pub struct OperatorBuilder {
    operator_instance: OperatorInstance,
}

impl OperatorBuilder {
    pub fn new(op_in: OperatorInstance) -> OperatorBuilder {
        OperatorBuilder {
            operator_instance: op_in,
        }
    }

    pub fn build() -> Result<()> {
        Ok(())
    }
}
