use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const MAX_RING_BYTES: usize = 1_000_000;

/// offset 语义：绝对字节序号（自 daemon 启动起写入的日志总字节数）。
/// 不变式：buf.len() == total_written - total_trimmed。
#[derive(Clone)]
pub struct RingWriter {
    inner: Arc<Mutex<RingState>>,
}

struct RingState {
    buf: Vec<u8>,
    total_written: u64,
    total_trimmed: u64,
}

pub struct LogChunk {
    pub bytes: Vec<u8>,
    /// 下次读取应使用的绝对 offset（即当前 total_written）。
    pub next_offset: u64,
    /// 请求的 offset 太旧（已被 trim 覆盖），本次从最旧可用处继续。
    pub truncated: bool,
}

impl RingWriter {
    pub fn new() -> Self {
        RingWriter {
            inner: Arc::new(Mutex::new(RingState {
                buf: Vec::with_capacity(MAX_RING_BYTES),
                total_written: 0,
                total_trimmed: 0,
            })),
        }
    }

    pub fn read_since(&self, abs_offset: u64) -> LogChunk {
        let st = self.inner.lock().unwrap();
        if abs_offset >= st.total_written {
            return LogChunk {
                bytes: Vec::new(),
                next_offset: st.total_written,
                truncated: false,
            };
        }
        if abs_offset < st.total_trimmed {
            return LogChunk {
                bytes: st.buf.clone(),
                next_offset: st.total_written,
                truncated: true,
            };
        }
        let start = (abs_offset - st.total_trimmed) as usize;
        LogChunk {
            bytes: st.buf[start..].to_vec(),
            next_offset: st.total_written,
            truncated: false,
        }
    }
}

impl Write for RingWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut st = self.inner.lock().unwrap();
        st.buf.extend_from_slice(data);
        st.total_written += data.len() as u64;
        if st.buf.len() > MAX_RING_BYTES {
            let trim = st.buf.len() - MAX_RING_BYTES;
            // 尽量切在换行后；找不到换行就硬切 trim，不清空全部
            let cut = st.buf[trim..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|p| trim + p + 1)
                .unwrap_or(trim);
            st.buf.drain(..cut);
            st.total_trimmed += cut as u64;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RingWriter {
    type Writer = RingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

pub fn init_logging() -> RingWriter {
    let ring = RingWriter::new();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let stdout_layer = tracing_subscriber::fmt::layer().with_filter(env_filter);

    let ring_layer = tracing_subscriber::fmt::layer()
        .with_writer(ring.clone())
        .with_ansi(false)
        .with_target(false)
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(ring_layer)
        .init();

    tracing::info!("logging initialized (in-memory ring buffer)");
    ring
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_since_absolute_offset() {
        let mut ring = RingWriter::new();
        ring.write_all(b"hello\n").unwrap();
        let chunk = ring.read_since(0);
        assert_eq!(chunk.bytes, b"hello\n");
        assert_eq!(chunk.next_offset, 6);
        assert!(!chunk.truncated);

        let chunk = ring.read_since(3);
        assert_eq!(chunk.bytes, b"lo\n");
        assert_eq!(chunk.next_offset, 6);
        assert!(!chunk.truncated);

        let chunk = ring.read_since(6);
        assert!(chunk.bytes.is_empty());
        assert_eq!(chunk.next_offset, 6);
        assert!(!chunk.truncated);

        // offset 超过已写入总量：空、不截断
        let chunk = ring.read_since(999);
        assert!(chunk.bytes.is_empty());
        assert_eq!(chunk.next_offset, 6);
        assert!(!chunk.truncated);
    }

    #[test]
    fn trim_marks_truncated_and_preserves_absolute_offsets() {
        let mut ring = RingWriter::new();
        // 写入超过 MAX_RING_BYTES 触发 trim
        let line = vec![b'x'; 1000];
        let mut written = 0u64;
        while written < (MAX_RING_BYTES + 5000) as u64 {
            ring.write_all(&line).unwrap();
            ring.write_all(b"\n").unwrap();
            written += 1001;
        }
        let total = {
            let st = ring.inner.lock().unwrap();
            assert!(st.total_trimmed > 0, "应已发生 trim");
            assert_eq!(st.buf.len() as u64, st.total_written - st.total_trimmed);
            (st.total_written, st.total_trimmed)
        };
        // 请求一个已被覆盖的旧 offset → truncated，从最旧可用处继续
        let chunk = ring.read_since(0);
        assert!(chunk.truncated);
        assert_eq!(chunk.next_offset, total.0);
        assert!(!chunk.bytes.is_empty());
        // 从最旧可用 offset 读 → 不 truncated
        let chunk = ring.read_since(total.1);
        assert!(!chunk.truncated);
        assert_eq!(chunk.next_offset, total.0);
    }
}
