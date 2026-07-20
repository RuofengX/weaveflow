pub mod resolver;
pub mod scope;

pub use resolver::{resolve_inputs, resolve_ref, resolve_value_tree};
pub use scope::{Scope, redact_env_values};
