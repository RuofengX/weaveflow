use async_trait::async_trait;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use super::http_client;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct CommandOperator;

fn inherit_env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

#[async_trait]
impl Operator for CommandOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("command", "执行 shell 命令").with_cache(false)
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        let cmd = inputs
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("command 算子需要 command 字段".into()))?;
        debug!(cmd = %cmd, "command execution");

        let shell = inputs
            .get("shell")
            .and_then(|v| v.as_str())
            .unwrap_or("sh");

        let stdin_data = inputs
            .get("stdin")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut child = tokio::process::Command::new(shell)
            .arg("-c")
            .arg(cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_clear()
            .env("PATH", inherit_env("PATH"))
            .env("HOME", inherit_env("HOME"))
            .env("LANG", inherit_env("LANG"))
            .env("LC_ALL", inherit_env("LC_ALL"))
            .env("TZ", inherit_env("TZ"))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| OperatorError::Runtime(format!("spawn {shell}: {e}")))?;

        let mut stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        if let Some(input) = stdin_data
            && let Some(mut s) = stdin.take() {
                tokio::spawn(async move {
                    let _ = s.write_all(input.as_bytes()).await;
                });
            }
        drop(stdin);

        let max_bytes = http_client::MAX_STDIO_BYTES;
        let stdout_task = tokio::spawn(read_capped(stdout, max_bytes));
        let stderr_task = tokio::spawn(read_capped(stderr, max_bytes));

        let output = child
            .wait()
            .await
            .map_err(|e| OperatorError::Runtime(format!("wait {cmd}: {e}")))?;

        let (stdout_buf, stdout_truncated) = stdout_task
            .await
            .map_err(|e| OperatorError::Runtime(format!("join stdout reader: {e}")))?
            .map_err(|e| OperatorError::Runtime(format!("read stdout: {e}")))?;
        let (stderr_buf, stderr_truncated) = stderr_task
            .await
            .map_err(|e| OperatorError::Runtime(format!("join stderr reader: {e}")))?
            .map_err(|e| OperatorError::Runtime(format!("read stderr: {e}")))?;

        let truncated = stdout_truncated || stderr_truncated;

        let mut stdout = String::from_utf8_lossy(&stdout_buf).into_owned();
        let mut stderr = String::from_utf8_lossy(&stderr_buf).into_owned();
        if stdout_truncated {
            stdout.push_str("\n[weave: stdout truncated at 10MB]");
        }
        if stderr_truncated {
            stderr.push_str("\n[weave: stderr truncated at 10MB]");
        }

        Ok(serde_json::json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": output.code().unwrap_or(-1),
            "success": output.success(),
            "truncated": truncated,
        }))
    }
}

/// 读满上限后继续 drain 丢弃，保证子进程不会阻塞在管道 write 上。
/// 返回保留的前 max 字节与是否截断（总量 > max 才算截断）。
async fn read_capped<R>(mut reader: R, max: usize) -> std::io::Result<(Vec<u8>, bool)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut total = 0usize;
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        total += n;
        if buf.len() < max {
            let keep = (max - buf.len()).min(n);
            buf.extend_from_slice(&chunk[..keep]);
        }
    }
    Ok((buf, total > max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn oversized_output_truncates_and_completes() {
        let op = CommandOperator;
        let out = op
            .run(json!({ "command": "head -c 11000000 /dev/zero" }))
            .await
            .expect("run");
        assert_eq!(out["truncated"], json!(true));
        assert_eq!(out["success"], json!(true));
        let stdout = out["stdout"].as_str().expect("stdout string");
        assert!(stdout.contains("truncated at 10MB"));
        assert!(stdout.len() <= http_client::MAX_STDIO_BYTES + 64);
    }

    #[tokio::test]
    async fn exactly_at_limit_is_not_truncated() {
        let op = CommandOperator;
        let out = op
            .run(json!({ "command": "head -c 10485760 /dev/zero" }))
            .await
            .expect("run");
        assert_eq!(out["truncated"], json!(false));
    }

    #[tokio::test]
    async fn small_output_not_truncated() {
        let op = CommandOperator;
        let out = op.run(json!({ "command": "echo hi" })).await.expect("run");
        assert_eq!(out["truncated"], json!(false));
        assert_eq!(out["stdout"], json!("hi\n"));
    }
}
