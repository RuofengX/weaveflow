use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct CommandOperator;

#[async_trait]
impl Operator for CommandOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("command", "执行 shell 命令").with_cache(false)
    }

    async fn run<'a>(
        &self,
        _data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let cmd = config
            .get("command")
            .or_else(|| config.get("cmd"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("command 算子需要 command 字段".into()))?;

        let shell = config
            .get("shell")
            .and_then(|v| v.as_str())
            .unwrap_or("sh");

        let stdin_data = config
            .get("stdin")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut child = tokio::process::Command::new(shell)
            .arg("-c")
            .arg(cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| OperatorError::Runtime(format!("spawn {shell}: {e}")))?;

        if let Some(input) = &stdin_data {
            use tokio::io::AsyncWriteExt;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(input.as_bytes())
                    .await
                    .map_err(|e| OperatorError::Runtime(format!("stdin write: {e}")))?;
            }
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| OperatorError::Runtime(format!("wait {cmd}: {e}")))?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let result = serde_json::json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": output.status.code().unwrap_or(-1),
            "success": output.status.success(),
        });

        let bytes = serde_json::to_vec(&result)
            .map_err(|e| OperatorError::Runtime(format!("command serialize: {e}")))?;
        Ok(Cow::Owned(bytes))
    }
}
