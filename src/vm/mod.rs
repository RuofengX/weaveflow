pub mod resolver;
pub mod scope;

pub use resolver::{resolve_inputs, resolve_ref};
pub use scope::{Scope, redact_env_values};
