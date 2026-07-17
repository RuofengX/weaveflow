pub mod scope;
pub mod resolver;

pub use scope::{Scope, redact_env_values};
pub use resolver::{resolve_inputs, resolve_ref};
