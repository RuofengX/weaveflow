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
            .or_else(|| inputs.get("cmd"))
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
            .spawn()
            .map_err(|e| OperatorError::Runtime(format!("spawn {shell}: {e}")))?;

        let mut stdin = child.stdin.take();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();

        if let Some(input) = stdin_data {
            if let Some(mut s) = stdin.take() {
                tokio::spawn(async move {
                    let _ = s.write_all(input.as_bytes()).await;
                });
            }
        }
        drop(stdin);

        let max_bytes = http_client::MAX_STDIO_BYTES as u64;
        let stdout_fut = async {
            let mut buf = Vec::new();
            let taken = (&mut stdout).take(max_bytes);
            tokio::pin!(taken);
            taken.read_to_end(&mut buf).await?;
            Ok::<_, std::io::Error>(buf)
        };
        let stderr_fut = async {
            let mut buf = Vec::new();
            let taken = (&mut stderr).take(max_bytes);
            tokio::pin!(taken);
            taken.read_to_end(&mut buf).await?;
            Ok::<_, std::io::Error>(buf)
        };

        let (stdout_result, stderr_result, output_result) = tokio::join!(
            stdout_fut,
            stderr_fut,
            child.wait(),
        );

        let stdout_buf = stdout_result.map_err(|e| {
            OperatorError::Runtime(format!("read stdout: {e}"))
        })?;
        let stderr_buf = stderr_result.map_err(|e| {
            OperatorError::Runtime(format!("read stderr: {e}"))
        })?;
        let output = output_result
            .map_err(|e| OperatorError::Runtime(format!("wait {cmd}: {e}")))?;

        let stdout_truncated = stdout_buf.len() >= http_client::MAX_STDIO_BYTES;
        let stderr_truncated = stderr_buf.len() >= http_client::MAX_STDIO_BYTES;

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
        }))
    }
}
