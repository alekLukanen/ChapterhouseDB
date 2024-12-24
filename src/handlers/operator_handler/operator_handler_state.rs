use crate::planner;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Queued,
    Running,
    Complete,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct OperatorInstance {
    id: u128,
    status: Status,
    operator: Option<planner::Operator>,
}

impl OperatorInstance {
    fn operator_compute(&self) -> Option<planner::OperatorCompute> {
        if let Some(op) = &self.operator {
            Some(op.compute.clone())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct TotalOperatorCompute {
    pub instances: usize,
    pub memory_in_mib: usize,
    pub cpu_in_thousandths: usize,
}

impl TotalOperatorCompute {
    pub fn any_greater_than(&self, c: &TotalOperatorCompute) -> bool {
        if self.instances > c.instances {
            true
        } else if self.memory_in_mib > c.memory_in_mib {
            true
        } else if self.cpu_in_thousandths > c.cpu_in_thousandths {
            true
        } else {
            false
        }
    }
    pub fn add(&mut self, c: &TotalOperatorCompute) -> &Self {
        self.instances += c.instances;
        self.memory_in_mib += c.memory_in_mib;
        self.cpu_in_thousandths += c.cpu_in_thousandths;
        self
    }
}

pub struct OperatorHandlerState {
    operator_instances: Vec<OperatorInstance>,
    allowed_compute: TotalOperatorCompute,
}

impl OperatorHandlerState {
    pub fn new(allowed_compute: TotalOperatorCompute) -> OperatorHandlerState {
        OperatorHandlerState {
            operator_instances: Vec::new(),
            allowed_compute,
        }
    }

    pub fn total_operator_compute(&self) -> TotalOperatorCompute {
        let mut total_compute = TotalOperatorCompute {
            instances: 0,
            memory_in_mib: 0,
            cpu_in_thousandths: 0,
        };
        for item in &self.operator_instances {
            if let Some(op) = &item.operator {
                total_compute.instances += 1;
                total_compute.memory_in_mib += op.compute.memory_in_mib;
                total_compute.cpu_in_thousandths += op.compute.cpu_in_thousandths;
            }
        }

        total_compute
    }

    pub fn get_allowed_compute(&self) -> TotalOperatorCompute {
        self.allowed_compute.clone()
    }
}
