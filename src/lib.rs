pub mod dsl;
pub mod engine;
pub mod error;
pub mod operator;
pub mod quickjs;
pub mod store;
pub mod tracker;
pub mod routine;
pub mod vm;

pub use engine::dag::Dag;
pub use engine::runner::Runner;
pub use vm::Scope;

/// 构建码：git 提交短哈希 6 位（build.rs 注入；非 git 构建为 "unknown"）。
/// 用于 CLI ↔ daemon 版本核对，识别旧版本 daemon 残留。
pub const BUILD_CODE: &str = match option_env!("WEAVEFLOW_BUILD_CODE") {
    Some(s) => s,
    None => "unknown",
};

/// CLI 与 daemon 构建码是否需要告警：两边都已知且不一致。
pub fn build_code_mismatch(cli: &str, daemon: &str) -> bool {
    cli != "unknown" && daemon != "unknown" && cli != daemon
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_code_mismatch_rules() {
        assert!(build_code_mismatch("abc123", "def456"));
        assert!(!build_code_mismatch("abc123", "abc123"));
        assert!(!build_code_mismatch("unknown", "abc123"));
        assert!(!build_code_mismatch("abc123", "unknown"));
    }
}
