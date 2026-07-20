use std::process::Command;

fn main() {
    // 用 git 提交短哈希（6 位）作为构建码：CLI 与 daemon 比对，
    // 不一致说明 daemon 可能是旧版本残留（如改名前的 weave）。
    println!("cargo:rerun-if-changed=.git/HEAD");
    let code = Command::new("git")
        .args(["rev-parse", "--short=6", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=WEAVEFLOW_BUILD_CODE={code}");
}
