pub mod builtin;
pub mod registry;
pub mod r#trait;

pub use registry::{builtins, get_builtin};
pub use r#trait::{Operator, OperatorError, OperatorSpec};
